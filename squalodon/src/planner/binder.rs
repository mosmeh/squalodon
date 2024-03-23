use super::{Plan, PlanNode, PlannerError, PlannerResult};
use crate::{
    catalog::{self, AggregateFunction, Aggregator, CatalogRef},
    executor::{ExecutorError, ExecutorResult},
    lexer,
    parser::{self, BinaryOp, UnaryOp},
    planner,
    rows::ColumnIndex,
    storage::Table,
    types::Type,
    CatalogError, Storage, Value,
};
use std::collections::{HashMap, HashSet};

pub struct Binder<'txn, 'db, T: Storage> {
    catalog: &'txn CatalogRef<'txn, 'db, T>,
}

impl<'txn, 'db, T: Storage> Binder<'txn, 'db, T> {
    pub fn new(catalog: &'txn CatalogRef<'txn, 'db, T>) -> Self {
        Self { catalog }
    }

    pub fn bind(&self, statement: parser::Statement) -> PlannerResult<Plan<'txn, 'db, T>> {
        match statement {
            parser::Statement::Explain(statement) => self.bind_explain(*statement),
            parser::Statement::Transaction(_) => unreachable!("handled before binding"),
            parser::Statement::ShowTables => {
                self.rewrite_to("SELECT * FROM squalodon_tables() ORDER BY name")
            }
            parser::Statement::Describe(name) => self.rewrite_to(&format!(
                "SELECT column_name, type, is_nullable, is_primary_key
                FROM squalodon_columns()
                WHERE table_name = {}",
                lexer::quote(&name, '\'')
            )),
            parser::Statement::CreateTable(create_table) => self.bind_create_table(create_table),
            parser::Statement::DropTable(drop_table) => self.bind_drop_table(drop_table),
            parser::Statement::Select(select) => self.bind_select(select),
            parser::Statement::Insert(insert) => self.bind_insert(insert),
            parser::Statement::Update(update) => self.bind_update(update),
            parser::Statement::Delete(delete) => self.bind_delete(delete),
        }
    }

    fn rewrite_to(&self, sql: &str) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut parser = parser::Parser::new(sql);
        let statement = parser.next().unwrap().unwrap();
        assert!(parser.next().is_none());
        self.bind(statement)
    }

    fn bind_explain(&self, statement: parser::Statement) -> PlannerResult<Plan<'txn, 'db, T>> {
        let plan = self.bind(statement)?;
        Ok(Plan {
            node: PlanNode::Explain(Box::new(plan.node)),
            schema: vec![planner::Column::new("plan", Type::Text)].into(),
        })
    }

    fn bind_create_table(
        &self,
        create_table: parser::CreateTable,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut column_names = HashSet::new();
        for column in &create_table.columns {
            if !column_names.insert(column.name.as_str()) {
                return Err(PlannerError::DuplicateColumn(column.name.clone()));
            }
        }
        match self.catalog.table(create_table.name.clone()) {
            Ok(_) if create_table.if_not_exists => return Ok(Plan::empty_source()),
            Ok(_) => return Err(PlannerError::TableAlreadyExists(create_table.name)),
            Err(CatalogError::UnknownEntry(_, _)) => (),
            Err(err) => return Err(err.into()),
        };
        let mut bound_constraints = HashSet::new();
        let mut has_primary_key = false;
        for constraint in create_table.constraints {
            match constraint {
                parser::Constraint::PrimaryKey(columns) => {
                    if has_primary_key {
                        return Err(PlannerError::MultiplePrimaryKeys);
                    }
                    has_primary_key = true;
                    let mut indices = Vec::with_capacity(columns.len());
                    let mut column_names = HashSet::new();
                    for column in &columns {
                        if !column_names.insert(column.as_str()) {
                            return Err(PlannerError::DuplicateColumn(column.clone()));
                        }
                        let (index, _) = create_table
                            .columns
                            .iter()
                            .enumerate()
                            .find(|(_, def)| def.name == *column)
                            .ok_or_else(|| PlannerError::UnknownColumn(column.clone()))?;
                        let index = ColumnIndex(index);
                        indices.push(index);
                        bound_constraints.insert(catalog::Constraint::NotNull(index));
                    }
                    bound_constraints.insert(catalog::Constraint::PrimaryKey(indices));
                }
                parser::Constraint::NotNull(column) => {
                    let (index, _) = create_table
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_, def)| def.name == column)
                        .ok_or_else(|| PlannerError::UnknownColumn(column.clone()))?;
                    bound_constraints.insert(catalog::Constraint::NotNull(ColumnIndex(index)));
                }
            }
        }
        if !has_primary_key {
            return Err(PlannerError::NoPrimaryKey);
        }
        let create_table = planner::CreateTable {
            name: create_table.name,
            columns: create_table.columns,
            constraints: bound_constraints.into_iter().collect(),
        };
        Ok(Plan::sink(PlanNode::CreateTable(create_table)))
    }

    fn bind_drop_table(&self, drop_table: parser::DropTable) -> PlannerResult<Plan<'txn, 'db, T>> {
        match self.catalog.table(drop_table.name.clone()) {
            Ok(_) => Ok(Plan::sink(PlanNode::DropTable(planner::DropTable {
                name: drop_table.name,
            }))),
            Err(CatalogError::UnknownEntry(_, _)) if drop_table.if_exists => {
                Ok(Plan::empty_source())
            }
            Err(err) => Err(err.into()),
        }
    }

    fn bind_select(&self, select: parser::Select) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut plan = self.bind_table_ref(select.from)?;
        if let Some(where_clause) = select.where_clause {
            plan = self.bind_where_clause(plan, where_clause)?;
        }

        let mut aggregate_ctx = AggregateContext::default();

        // We need to determine whether the query is an aggregate query or not.
        // We search for all occurrences of aggregate functions in the query.
        if let Some(having) = &select.having {
            plan = aggregate_ctx.gather_aggregates(self.catalog, plan, having)?;
        }
        for order_by in &select.order_by {
            plan = aggregate_ctx.gather_aggregates(self.catalog, plan, &order_by.expr)?;
        }
        for projection in &select.projections {
            match projection {
                parser::Projection::Wildcard => (),
                parser::Projection::Expression { expr, .. } => {
                    plan = aggregate_ctx.gather_aggregates(self.catalog, plan, expr)?;
                }
            }
        }

        // The query is an aggregate query if there are any aggregate functions
        // or if there is a GROUP BY clause.
        if aggregate_ctx.has_aggregates() || !select.group_by.is_empty() {
            plan = aggregate_ctx.bind_aggregates(self.catalog, plan, select.group_by)?;
        }

        // HAVING, ORDER BY and SELECT expressions can reference
        // the aggregated expressions. They are bound with the special
        // binder that allows them to reference the aggregated expressions.
        let aggregated_expr_binder =
            ExpressionBinder::new(self.catalog).with_aggregate_context(aggregate_ctx);

        if let Some(having) = select.having {
            plan = self.bind_having(&aggregated_expr_binder, plan, having)?;
        }
        let plan = self.bind_order_by(&aggregated_expr_binder, plan, select.order_by)?;
        let plan = self.bind_limit(plan, select.limit, select.offset)?;
        let plan = self.bind_projections(&aggregated_expr_binder, plan, select.projections)?;
        Ok(plan)
    }

    fn bind_where_clause(
        &self,
        source: Plan<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let (plan, cond) = self.bind_expr(source, expr)?;
        let cond = cond.expect_type(Type::Boolean)?;
        Ok(plan.inherit_schema(|node| {
            PlanNode::Filter(planner::Filter {
                source: Box::new(node),
                cond,
            })
        }))
    }

    #[allow(clippy::unused_self)]
    fn bind_having(
        &self,
        expr_binder: &ExpressionBinder<'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let (plan, cond) = expr_binder.bind(source, expr)?;
        let cond = cond.expect_type(Type::Boolean)?;
        Ok(plan.inherit_schema(|node| {
            PlanNode::Filter(planner::Filter {
                source: Box::new(node),
                cond,
            })
        }))
    }

    #[allow(clippy::unused_self)]
    fn bind_projections(
        &self,
        expr_binder: &ExpressionBinder<'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        projections: Vec<parser::Projection>,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut plan = source;
        let mut exprs = Vec::new();
        let mut columns = Vec::new();
        for projection in projections {
            match projection {
                parser::Projection::Wildcard => {
                    for (i, column) in plan.schema.0.iter().cloned().enumerate() {
                        exprs.push(planner::Expression::ColumnRef {
                            index: ColumnIndex(i),
                        });
                        columns.push(column);
                    }
                }
                parser::Projection::Expression { expr, alias } => {
                    let (new_plan, bound_expr) = expr_binder.bind(plan, expr.clone())?;
                    plan = new_plan;
                    let table_name;
                    let column_name;
                    if let Some(alias) = alias {
                        table_name = None;
                        column_name = alias;
                    } else if let planner::Expression::ColumnRef { index } = &bound_expr.expr {
                        let column = &plan.schema.0[index.0];
                        table_name = column.table_name.clone();
                        column_name = column.column_name.clone();
                    } else {
                        table_name = None;
                        column_name = expr.to_string();
                    }
                    exprs.push(bound_expr.expr);
                    columns.push(planner::Column {
                        table_name,
                        column_name,
                        ty: bound_expr.ty,
                    });
                }
            }
        }
        Ok(Plan {
            node: PlanNode::Project(planner::Project {
                source: Box::new(plan.node),
                exprs,
            }),
            schema: columns.into(),
        })
    }

    #[allow(clippy::unused_self)]
    fn bind_order_by(
        &self,
        expr_binder: &ExpressionBinder<'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        order_by: Vec<parser::OrderBy>,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        if order_by.is_empty() {
            return Ok(source);
        }
        let mut plan = source;
        let mut bound_order_by = Vec::with_capacity(order_by.len());
        for item in order_by {
            let (new_plan, TypedExpression { expr, .. }) = expr_binder.bind(plan, item.expr)?;
            plan = new_plan;
            bound_order_by.push(planner::OrderBy {
                expr,
                order: item.order,
                null_order: item.null_order,
            });
        }
        Ok(plan.inherit_schema(|node| {
            PlanNode::Sort(planner::Sort {
                source: node.into(),
                order_by: bound_order_by,
            })
        }))
    }

    fn bind_limit(
        &self,
        source: Plan<'txn, 'db, T>,
        limit: Option<parser::Expression>,
        offset: Option<parser::Expression>,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        if limit.is_none() && offset.is_none() {
            return Ok(source);
        }
        let limit = self.bind_limit_expr(limit)?;
        let offset = self.bind_limit_expr(offset)?;
        Ok(source.inherit_schema(|node| {
            PlanNode::Limit(planner::Limit {
                source: Box::new(node),
                limit,
                offset,
            })
        }))
    }

    fn bind_limit_expr(
        &self,
        expr: Option<parser::Expression>,
    ) -> PlannerResult<Option<planner::Expression>> {
        let Some(expr) = expr else {
            return Ok(None);
        };
        let expr = self
            .bind_expr_without_source(expr)?
            .expect_type(Type::Integer)?;
        Ok(Some(expr))
    }

    fn bind_insert(&self, insert: parser::Insert) -> PlannerResult<Plan<'txn, 'db, T>> {
        let table = self.catalog.table(insert.table_name)?;
        let Plan { node, schema } = self.bind_select(insert.select)?;
        if table.columns().len() != schema.0.len() {
            return Err(PlannerError::ColumnCountMismatch {
                expected: table.columns().len(),
                actual: schema.0.len(),
            });
        }
        let node = if let Some(column_names) = insert.column_names {
            if table.columns().len() != column_names.len() {
                return Err(PlannerError::ColumnCountMismatch {
                    expected: table.columns().len(),
                    actual: column_names.len(),
                });
            }
            let mut indices_in_source = vec![None; table.columns().len()];
            let iter = column_names.into_iter().zip(schema.0).enumerate();
            for (index_in_source, (column_name, actual_column)) in iter {
                let (index_in_table, expected_column) = table
                    .columns()
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name == column_name)
                    .ok_or_else(|| PlannerError::UnknownColumn(column_name.clone()))?;
                match &mut indices_in_source[index_in_table] {
                    Some(_) => return Err(PlannerError::DuplicateColumn(column_name)),
                    i @ None => *i = Some(index_in_source),
                }
                match actual_column.ty {
                    Some(ty) if ty != expected_column.ty => return Err(PlannerError::TypeError),
                    _ => (),
                }
            }
            let exprs = indices_in_source
                .into_iter()
                .map(|i| planner::Expression::ColumnRef {
                    index: ColumnIndex(i.unwrap()),
                })
                .collect();
            PlanNode::Project(planner::Project {
                source: Box::new(node),
                exprs,
            })
        } else {
            for (actual, expected) in schema.0.iter().zip(table.columns()) {
                match actual.ty {
                    Some(actual) if actual != expected.ty => return Err(PlannerError::TypeError),
                    _ => (),
                }
            }
            node
        };
        Ok(Plan::sink(PlanNode::Insert(planner::Insert {
            source: Box::new(node),
            table,
        })))
    }

    fn bind_update(&self, update: parser::Update) -> PlannerResult<Plan<'txn, 'db, T>> {
        let table = self.catalog.table(update.table_name)?;
        let mut plan = self.bind_base_table(table.clone());
        if let Some(where_clause) = update.where_clause {
            plan = self.bind_where_clause(plan, where_clause)?;
        }
        let mut exprs = vec![None; table.columns().len()];
        for set in update.sets {
            let (index, column) = plan.schema.resolve_column(&planner::ColumnRef {
                table_name: Some(table.name().to_owned()),
                column_name: set.column_name.clone(),
            })?;
            let expr = &mut exprs[index.0];
            match expr {
                Some(_) => return Err(PlannerError::DuplicateColumn(column.column_name.clone())),
                None => {
                    let expected_type = column.ty;
                    let (new_plan, bound_expr) = self.bind_expr(plan, set.expr)?;
                    *expr = bound_expr.expect_type(expected_type)?.into();
                    plan = new_plan;
                }
            }
        }
        let exprs = exprs
            .into_iter()
            .enumerate()
            .map(|(i, expr)| {
                expr.unwrap_or(planner::Expression::ColumnRef {
                    index: ColumnIndex(i),
                })
            })
            .collect();
        let node = PlanNode::Project(planner::Project {
            source: Box::new(plan.node),
            exprs,
        });
        Ok(Plan::sink(PlanNode::Update(planner::Update {
            source: Box::new(node),
            table,
        })))
    }

    fn bind_delete(&self, delete: parser::Delete) -> PlannerResult<Plan<'txn, 'db, T>> {
        let table = self.catalog.table(delete.table_name)?;
        let mut plan = self.bind_base_table(table.clone());
        if let Some(where_clause) = delete.where_clause {
            plan = self.bind_where_clause(plan, where_clause)?;
        }
        Ok(Plan::sink(PlanNode::Delete(planner::Delete {
            source: Box::new(plan.node),
            table,
        })))
    }

    fn bind_table_ref(&self, table_ref: parser::TableRef) -> PlannerResult<Plan<'txn, 'db, T>> {
        match table_ref {
            parser::TableRef::BaseTable { name } => {
                Ok(self.bind_base_table(self.catalog.table(name)?))
            }
            parser::TableRef::Join(join) => self.bind_join(*join),
            parser::TableRef::Subquery(select) => self.bind_select(*select),
            parser::TableRef::Function { name, args } => self.bind_table_function(name, args),
            parser::TableRef::Values(values) => self.bind_values(values),
        }
    }

    #[allow(clippy::unused_self)]
    fn bind_base_table(&self, table: Table<'txn, 'db, T>) -> Plan<'txn, 'db, T> {
        let columns: Vec<_> = table
            .columns()
            .iter()
            .cloned()
            .map(|column| planner::Column {
                table_name: Some(table.name().to_owned()),
                column_name: column.name,
                ty: Some(column.ty),
            })
            .collect();
        Plan {
            node: PlanNode::Scan(planner::Scan::SeqScan { table }),
            schema: columns.into(),
        }
    }

    fn bind_join(&self, join: parser::Join) -> PlannerResult<Plan<'txn, 'db, T>> {
        let left = self.bind_table_ref(join.left)?;
        let right = self.bind_table_ref(join.right)?;
        let mut schema = left.schema;
        schema.0.extend(right.schema.0);
        let plan = Plan {
            node: PlanNode::CrossProduct(planner::CrossProduct {
                left: Box::new(left.node),
                right: Box::new(right.node),
            }),
            schema,
        };
        let Some(on) = join.on else {
            return Ok(plan);
        };
        let (plan, expr) = self.bind_expr(plan, on)?;
        let on = expr.expect_type(Type::Boolean)?;
        Ok(plan.inherit_schema(|node| {
            PlanNode::Filter(planner::Filter {
                source: Box::new(node),
                cond: on,
            })
        }))
    }

    fn bind_table_function(
        &self,
        name: String,
        args: Vec<parser::Expression>,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let table_function = self.catalog.table_function(&name)?;
        let mut exprs = Vec::with_capacity(args.len());
        for (expr, column) in args.into_iter().zip(table_function.result_columns.iter()) {
            let expr = self
                .bind_expr_without_source(expr)?
                .expect_type(column.ty)?;
            exprs.push(expr);
        }
        let node = PlanNode::Project(planner::Project {
            source: Box::new(PlanNode::Values(planner::Values::one_empty_row())),
            exprs,
        });
        let columns: Vec<_> = table_function.result_columns.clone();
        Ok(Plan {
            node: PlanNode::Scan(planner::Scan::FunctionScan {
                source: Box::new(node),
                fn_ptr: table_function.fn_ptr,
            }),
            schema: columns.into(),
        })
    }

    fn bind_values(&self, values: parser::Values) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut rows = Vec::with_capacity(values.rows.len());
        let mut columns;
        let mut rows_iter = values.rows.into_iter();
        match rows_iter.next() {
            Some(row) => {
                let mut exprs = Vec::with_capacity(row.len());
                columns = Vec::with_capacity(row.len());
                let row = row.into_iter().enumerate();
                for (i, expr) in row {
                    let TypedExpression { expr, ty } = self.bind_expr_without_source(expr)?;
                    exprs.push(expr);
                    columns.push(planner::Column {
                        table_name: None,
                        column_name: format!("column{}", i + 1),
                        ty,
                    });
                }
                rows.push(exprs);
            }
            None => return Ok(Plan::empty_source()),
        }
        for row in rows_iter {
            assert_eq!(row.len(), columns.len());
            let mut exprs = Vec::with_capacity(row.len());
            for (expr, column) in row.into_iter().zip(columns.iter()) {
                let expr = self
                    .bind_expr_without_source(expr)?
                    .expect_type(column.ty)?;
                exprs.push(expr);
            }
            rows.push(exprs);
        }
        let node = PlanNode::Values(planner::Values { rows });
        Ok(Plan {
            node,
            schema: columns.into(),
        })
    }

    fn bind_expr(
        &self,
        source: Plan<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<(Plan<'txn, 'db, T>, TypedExpression)> {
        ExpressionBinder::new(self.catalog).bind(source, expr)
    }

    fn bind_expr_without_source(&self, expr: parser::Expression) -> PlannerResult<TypedExpression> {
        ExpressionBinder::new(self.catalog).bind_without_source(expr)
    }
}

struct TypedExpression {
    expr: planner::Expression,
    ty: Option<Type>,
}

impl TypedExpression {
    fn expect_type<T: Into<Option<Type>>>(self, expected: T) -> PlannerResult<planner::Expression> {
        if let (Some(actual), Some(expected)) = (self.ty, expected.into()) {
            if actual != expected {
                return Err(PlannerError::TypeError);
            }
        }
        Ok(self.expr)
    }
}

#[derive(Default)]
struct AggregateContext<'txn> {
    aggregates: HashMap<Aggregate, usize>,
    bound_aggregates: Vec<BoundAggregate<'txn>>,
}

impl<'txn> AggregateContext<'txn> {
    fn has_aggregates(&self) -> bool {
        !self.aggregates.is_empty()
    }

    fn gather_aggregates<'db, T: Storage>(
        &mut self,
        catalog: &'txn CatalogRef<'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        expr: &parser::Expression,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        self.gather_aggregates_inner(catalog, source, expr, false)
    }

    fn gather_aggregates_inner<'db, T: Storage>(
        &mut self,
        catalog: &'txn CatalogRef<'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        expr: &parser::Expression,
        in_aggregate_args: bool,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        match expr {
            parser::Expression::Constant(_)
            | parser::Expression::ColumnRef(_)
            | parser::Expression::ScalarSubquery(_) => Ok(source),
            parser::Expression::UnaryOp { expr, .. } => {
                self.gather_aggregates_inner(catalog, source, expr, in_aggregate_args)
            }
            parser::Expression::BinaryOp { lhs, rhs, .. } => {
                let plan = self.gather_aggregates_inner(catalog, source, lhs, in_aggregate_args)?;
                self.gather_aggregates_inner(catalog, plan, rhs, in_aggregate_args)
            }
            parser::Expression::Function { ref name, ref args } => {
                let function = match catalog.aggregate_function(name) {
                    Ok(func) => func,
                    Err(CatalogError::UnknownEntry(_, _)) => return Ok(source),
                    Err(err) => return Err(err.into()),
                };
                if in_aggregate_args {
                    return Err(PlannerError::AggregateNotAllowed);
                }
                let [arg] = args.as_slice() else {
                    return Err(PlannerError::ArityError);
                };
                let plan = self.gather_aggregates_inner(catalog, source, arg, true)?;
                let aggregate = Aggregate {
                    function_name: name.clone(),
                    arg: arg.clone(),
                };
                let std::collections::hash_map::Entry::Vacant(entry) =
                    self.aggregates.entry(aggregate)
                else {
                    return Ok(plan);
                };
                let (plan, bound_expr) = ExpressionBinder::new(catalog).bind(plan, arg.clone())?;
                let index = self.bound_aggregates.len();
                self.bound_aggregates.push(BoundAggregate {
                    function,
                    arg: bound_expr.expr,
                    result_column: planner::Column {
                        table_name: None,
                        column_name: expr.to_string(),
                        ty: (function.bind_fn_ptr)(bound_expr.ty)?,
                    },
                });
                entry.insert(index);
                Ok(plan)
            }
        }
    }

    fn bind_aggregates<'db, T: Storage>(
        &self,
        catalog: &'txn CatalogRef<'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        group_by: Vec<parser::Expression>,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut plan = source;
        let mut exprs = Vec::with_capacity(self.bound_aggregates.len());
        let mut columns = Vec::with_capacity(self.bound_aggregates.len());

        // The first `bound_aggregates.len()` columns are the aggregated columns
        // that are passed to the respective aggregate functions.
        for bound_aggregate in &self.bound_aggregates {
            exprs.push(bound_aggregate.arg.clone());
            columns.push(bound_aggregate.result_column.clone());
        }

        // The rest of the columns are the columns in the GROUP BY clause.
        let expr_binder = ExpressionBinder::new(catalog);
        for group_by in group_by {
            let (new_plan, TypedExpression { expr, ty }) =
                expr_binder.bind(plan, group_by.clone())?;
            plan = new_plan;
            let column = if let planner::Expression::ColumnRef { index } = expr {
                plan.schema.0[index.0].clone()
            } else {
                planner::Column {
                    table_name: None,
                    column_name: group_by.to_string(),
                    ty,
                }
            };
            exprs.push(expr);
            columns.push(column);
        }

        let node = PlanNode::Project(planner::Project {
            source: Box::new(plan.node),
            exprs,
        });

        let init_fn_ptrs = self
            .bound_aggregates
            .iter()
            .map(|bound_aggregate| bound_aggregate.function.init_fn_ptr)
            .collect();
        Ok(Plan {
            node: PlanNode::Aggregate(planner::Aggregate {
                source: Box::new(node),
                init_fn_ptrs,
            }),
            schema: columns.into(),
        })
    }
}

#[derive(PartialEq, Eq, Hash)]
struct Aggregate {
    function_name: String,
    arg: parser::Expression,
}

struct BoundAggregate<'a> {
    function: &'a AggregateFunction,
    arg: planner::Expression,
    result_column: planner::Column,
}

struct ExpressionBinder<'txn, 'db, T: Storage> {
    catalog: &'txn CatalogRef<'txn, 'db, T>,
    aggregate_ctx: Option<AggregateContext<'txn>>,
}

impl<'txn, 'db, T: Storage> ExpressionBinder<'txn, 'db, T> {
    fn new(catalog: &'txn CatalogRef<'txn, 'db, T>) -> Self {
        Self {
            catalog,
            aggregate_ctx: None,
        }
    }

    fn with_aggregate_context(self, aggregate_ctx: AggregateContext<'txn>) -> Self {
        Self {
            catalog: self.catalog,
            aggregate_ctx: Some(aggregate_ctx),
        }
    }

    fn bind(
        &self,
        source: Plan<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<(Plan<'txn, 'db, T>, TypedExpression)> {
        match expr {
            parser::Expression::Constant(value) => {
                let ty = value.ty();
                Ok((
                    source,
                    TypedExpression {
                        expr: planner::Expression::Constact(value),
                        ty,
                    },
                ))
            }
            parser::Expression::ColumnRef(column_ref) => {
                let (index, column) = source.schema.resolve_column(&column_ref)?;
                let expr = planner::Expression::ColumnRef { index };
                let ty = column.ty;
                Ok((source, TypedExpression { expr, ty }))
            }
            parser::Expression::UnaryOp { op, expr } => {
                let (plan, TypedExpression { expr, ty }) = self.bind(source, *expr)?;
                let ty = match (op, ty) {
                    (UnaryOp::Not, _) => Some(Type::Boolean),
                    (UnaryOp::Plus | UnaryOp::Minus, Some(ty)) if ty.is_numeric() => Some(ty),
                    (UnaryOp::Plus | UnaryOp::Minus, None) => None,
                    _ => return Err(PlannerError::TypeError),
                };
                let expr = planner::Expression::UnaryOp {
                    op,
                    expr: expr.into(),
                };
                Ok((plan, TypedExpression { expr, ty }))
            }
            parser::Expression::BinaryOp { op, lhs, rhs } => {
                let (plan, lhs) = self.bind(source, *lhs)?;
                let (plan, rhs) = self.bind(plan, *rhs)?;
                let ty = match op {
                    BinaryOp::Add
                    | BinaryOp::Sub
                    | BinaryOp::Mul
                    | BinaryOp::Div
                    | BinaryOp::Mod => match (lhs.ty, rhs.ty) {
                        (Some(Type::Integer), Some(Type::Integer)) => Some(Type::Integer),
                        (Some(ty), Some(Type::Real)) | (Some(Type::Real), Some(ty))
                            if ty.is_numeric() =>
                        {
                            Some(Type::Real)
                        }
                        (None, _) => rhs.ty,
                        (_, None) => lhs.ty,
                        _ => return Err(PlannerError::TypeError),
                    },
                    BinaryOp::Eq
                    | BinaryOp::Ne
                    | BinaryOp::Lt
                    | BinaryOp::Le
                    | BinaryOp::Gt
                    | BinaryOp::Ge
                    | BinaryOp::And
                    | BinaryOp::Or => match (lhs.ty, rhs.ty) {
                        (Some(lhs_ty), Some(rhs_ty)) if lhs_ty != rhs_ty => {
                            return Err(PlannerError::TypeError)
                        }
                        _ => Some(Type::Boolean),
                    },
                    BinaryOp::Concat => match (lhs.ty, rhs.ty) {
                        (Some(Type::Text) | None, Some(Type::Text) | None) => Some(Type::Text),
                        _ => return Err(PlannerError::TypeError),
                    },
                };
                let expr = planner::Expression::BinaryOp {
                    op,
                    lhs: lhs.expr.into(),
                    rhs: rhs.expr.into(),
                };
                Ok((plan, TypedExpression { expr, ty }))
            }
            parser::Expression::Function { name, args } => {
                // We currently assume all functions in expressions are
                // aggregate functions.
                self.catalog.aggregate_function(&name)?;
                Ok((source, self.resolve_aggregate(name, args)?))
            }
            parser::Expression::ScalarSubquery(select) => {
                /// An aggregator that asserts that the subquery returns
                /// a single row.
                #[derive(Default)]
                struct AssertSingleRow {
                    value: Option<Value>,
                }

                impl Aggregator for AssertSingleRow {
                    fn update(&mut self, value: &Value) -> ExecutorResult<()> {
                        if self.value.is_some() {
                            Err(ExecutorError::MultipleRowsFromSubquery)
                        } else {
                            self.value = Some(value.clone());
                            Ok(())
                        }
                    }

                    fn finish(&self) -> Value {
                        self.value.clone().unwrap_or(Value::Null)
                    }
                }

                fn init_agg() -> Box<dyn Aggregator> {
                    Box::<AssertSingleRow>::default()
                }

                let column_name = select.to_string();

                let subquery_plan = Binder::new(self.catalog).bind_select(*select)?;
                let [subquery_result_column] = subquery_plan
                    .schema
                    .0
                    .try_into()
                    .map_err(|_| PlannerError::MultipleColumnsFromSubquery)?;

                let subquery_node = PlanNode::Aggregate(planner::Aggregate {
                    source: Box::new(subquery_plan.node),
                    init_fn_ptrs: vec![init_agg],
                });

                let mut columns = source.schema.0.clone();
                columns.push(planner::Column {
                    table_name: None,
                    column_name,
                    ty: subquery_result_column.ty,
                });

                let plan = Plan {
                    node: PlanNode::CrossProduct(planner::CrossProduct {
                        left: Box::new(source.node),
                        right: Box::new(subquery_node),
                    }),
                    schema: columns.into(),
                };

                // CrossProduct appends the subquery result column to the right
                // of the source schema.
                let column_index = ColumnIndex(source.schema.0.len());

                Ok((
                    plan,
                    TypedExpression {
                        expr: planner::Expression::ColumnRef {
                            index: column_index,
                        },
                        ty: subquery_result_column.ty,
                    },
                ))
            }
        }
    }

    fn bind_without_source(&self, expr: parser::Expression) -> PlannerResult<TypedExpression> {
        self.bind(Plan::empty_source(), expr).map(|(_, e)| e)
    }

    fn resolve_aggregate(
        &self,
        name: String,
        args: Vec<parser::Expression>,
    ) -> PlannerResult<TypedExpression> {
        let Some(aggregate_ctx) = &self.aggregate_ctx else {
            return Err(PlannerError::AggregateNotAllowed);
        };
        let [arg] = args.try_into().map_err(|_| PlannerError::ArityError)?;
        let aggregate = Aggregate {
            function_name: name,
            arg,
        };
        let index = *aggregate_ctx.aggregates.get(&aggregate).unwrap();
        let bound_aggregate = &aggregate_ctx.bound_aggregates[index];
        Ok(TypedExpression {
            expr: planner::Expression::ColumnRef {
                index: ColumnIndex(index),
            },
            ty: bound_aggregate.result_column.ty,
        })
    }
}

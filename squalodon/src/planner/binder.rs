use super::{PlanNode, PlannerError, PlannerResult, TypedPlanNode, Values};
use crate::{
    catalog::{self, CatalogRef},
    parser::{self, BinaryOp, ColumnRef, UnaryOp},
    planner,
    rows::ColumnIndex,
    storage::Table,
    types::Type,
    CatalogError, Storage,
};
use planner::PlanSchema;
use std::collections::HashSet;

pub struct Binder<'txn, 'db, T: Storage> {
    catalog: &'txn CatalogRef<'txn, 'db, T>,
}

impl<'txn, 'db, T: Storage> Binder<'txn, 'db, T> {
    pub fn new(catalog: &'txn CatalogRef<'txn, 'db, T>) -> Self {
        Self { catalog }
    }

    pub fn bind(&self, statement: parser::Statement) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
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
                parser::quote(&name, '\'')
            )),
            parser::Statement::CreateTable(create_table) => self.bind_create_table(create_table),
            parser::Statement::DropTable(drop_table) => self.bind_drop_table(drop_table),
            parser::Statement::Select(select) => self.bind_select(select),
            parser::Statement::Insert(insert) => self.bind_insert(insert),
            parser::Statement::Update(update) => self.bind_update(update),
            parser::Statement::Delete(delete) => self.bind_delete(delete),
        }
    }

    fn rewrite_to(&self, sql: &str) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let mut parser = parser::Parser::new(sql);
        let statement = parser.next().unwrap().unwrap();
        assert!(parser.next().is_none());
        self.bind(statement)
    }

    fn bind_explain(
        &self,
        statement: parser::Statement,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let plan = self.bind(statement)?;
        Ok(TypedPlanNode {
            node: PlanNode::Explain(Box::new(plan.node)),
            schema: vec![planner::Column::new("plan", Type::Text)].into(),
        })
    }

    fn bind_create_table(
        &self,
        create_table: parser::CreateTable,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let mut column_names = HashSet::new();
        for column in &create_table.columns {
            if !column_names.insert(column.name.as_str()) {
                return Err(PlannerError::DuplicateColumn(column.name.clone()));
            }
        }
        match self.catalog.table(create_table.name.clone()) {
            Ok(_) if create_table.if_not_exists => return Ok(TypedPlanNode::empty_source()),
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
        Ok(TypedPlanNode::sink(PlanNode::CreateTable(create_table)))
    }

    fn bind_drop_table(
        &self,
        drop_table: parser::DropTable,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        match self.catalog.table(drop_table.name.clone()) {
            Ok(_) => Ok(TypedPlanNode::sink(PlanNode::DropTable(
                planner::DropTable {
                    name: drop_table.name,
                },
            ))),
            Err(CatalogError::UnknownEntry(_, _)) if drop_table.if_exists => {
                Ok(TypedPlanNode::empty_source())
            }
            Err(err) => Err(err.into()),
        }
    }

    fn bind_select(&self, select: parser::Select) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let mut node = self.bind_table_ref(select.from)?;
        if let Some(where_clause) = select.where_clause {
            node = self.bind_where_clause(node, where_clause)?;
        }
        let node = self.bind_order_by(node, select.order_by)?;
        let node = self.bind_limit(node, select.limit, select.offset)?;
        let node = self.bind_projections(node, select.projections)?;
        Ok(node)
    }

    fn bind_where_clause(
        &self,
        source: TypedPlanNode<'txn, 'db, T>,
        expr: parser::Expression,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let cond = self
            .bind_expr(&source.schema, expr)?
            .expect_type(Type::Boolean)?;
        Ok(source.inherit_schema(|source| {
            PlanNode::Filter(planner::Filter {
                source: Box::new(source),
                cond,
            })
        }))
    }

    fn bind_projections(
        &self,
        source: TypedPlanNode<'txn, 'db, T>,
        projections: Vec<parser::Projection>,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let mut exprs = Vec::new();
        let mut columns = Vec::new();
        for projection in projections {
            match projection {
                parser::Projection::Wildcard => {
                    for (i, column) in source.schema.0.iter().cloned().enumerate() {
                        exprs.push(planner::Expression::ColumnRef {
                            index: ColumnIndex(i),
                        });
                        columns.push(column);
                    }
                }
                parser::Projection::Expression { expr, alias } => {
                    let TypedExpression { expr, ty } = self.bind_expr(&source.schema, expr)?;
                    let table_name;
                    let column_name;
                    if let Some(alias) = alias {
                        table_name = None;
                        column_name = alias;
                    } else if let planner::Expression::ColumnRef { index } = &expr {
                        let column = &source.schema.0[index.0];
                        table_name = column.table_name.clone();
                        column_name = column.column_name.clone();
                    } else {
                        table_name = None;
                        column_name = expr.to_string();
                    }
                    exprs.push(expr);
                    columns.push(planner::Column {
                        table_name,
                        column_name,
                        ty,
                    });
                }
            }
        }
        Ok(TypedPlanNode {
            node: PlanNode::Project(planner::Project {
                source: Box::new(source.node),
                exprs,
            }),
            schema: columns.into(),
        })
    }

    fn bind_order_by(
        &self,
        source: TypedPlanNode<'txn, 'db, T>,
        order_by: Vec<parser::OrderBy>,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        if order_by.is_empty() {
            return Ok(source);
        }
        let mut bound_order_by = Vec::with_capacity(order_by.len());
        for item in order_by {
            let TypedExpression { expr, .. } = self.bind_expr(&source.schema, item.expr)?;
            bound_order_by.push(planner::OrderBy {
                expr,
                order: item.order,
                null_order: item.null_order,
            });
        }
        Ok(source.inherit_schema(|source| {
            PlanNode::Sort(planner::Sort {
                source: source.into(),
                order_by: bound_order_by,
            })
        }))
    }

    fn bind_limit(
        &self,
        source: TypedPlanNode<'txn, 'db, T>,
        limit: Option<parser::Expression>,
        offset: Option<parser::Expression>,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        if limit.is_none() && offset.is_none() {
            return Ok(source);
        }
        let limit = self.bind_limit_expr(limit)?;
        let offset = self.bind_limit_expr(offset)?;
        Ok(source.inherit_schema(|source| {
            PlanNode::Limit(planner::Limit {
                source: Box::new(source),
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
            .bind_expr(&PlanSchema::empty(), expr)?
            .expect_type(Type::Integer)?;
        Ok(Some(expr))
    }

    fn bind_insert(&self, insert: parser::Insert) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let table = self.catalog.table(insert.table_name)?;
        let TypedPlanNode { node, schema } = self.bind_select(insert.select)?;
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
        Ok(TypedPlanNode::sink(PlanNode::Insert(planner::Insert {
            source: Box::new(node),
            table,
        })))
    }

    fn bind_update(&self, update: parser::Update) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let table = self.catalog.table(update.table_name)?;
        let mut node = self.bind_base_table(table.clone());
        if let Some(where_clause) = update.where_clause {
            node = self.bind_where_clause(node, where_clause)?;
        }
        let mut exprs = vec![None; table.columns().len()];
        for set in update.sets {
            let (index, column) = node.schema.resolve_column(&ColumnRef {
                table_name: Some(table.name().to_owned()),
                column_name: set.column_name.clone(),
            })?;
            let expr = &mut exprs[index.0];
            match expr {
                Some(_) => return Err(PlannerError::DuplicateColumn(column.column_name.clone())),
                None => {
                    *expr = self
                        .bind_expr(&node.schema, set.expr)?
                        .expect_type(column.ty)?
                        .into();
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
            source: Box::new(node.node),
            exprs,
        });
        Ok(TypedPlanNode::sink(PlanNode::Update(planner::Update {
            source: Box::new(node),
            table,
        })))
    }

    fn bind_delete(&self, delete: parser::Delete) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let table = self.catalog.table(delete.table_name)?;
        let mut node = self.bind_base_table(table.clone());
        if let Some(where_clause) = delete.where_clause {
            node = self.bind_where_clause(node, where_clause)?;
        }
        Ok(TypedPlanNode::sink(PlanNode::Delete(planner::Delete {
            source: Box::new(node.node),
            table,
        })))
    }

    fn bind_table_ref(
        &self,
        table_ref: parser::TableRef,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
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
    fn bind_base_table(&self, table: Table<'txn, 'db, T>) -> TypedPlanNode<'txn, 'db, T> {
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
        TypedPlanNode {
            node: PlanNode::Scan(planner::Scan::SeqScan { table }),
            schema: columns.into(),
        }
    }

    fn bind_join(&self, join: parser::Join) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let left = self.bind_table_ref(join.left)?;
        let right = self.bind_table_ref(join.right)?;
        let mut schema = left.schema;
        schema.0.extend(right.schema.0);
        let on = match join.on {
            Some(on) => self.bind_expr(&schema, on)?.expect_type(Type::Boolean)?,
            None => planner::Expression::Constact(true.into()),
        };
        Ok(TypedPlanNode {
            node: PlanNode::Join(planner::Join::NestedLoop {
                left: Box::new(left.node),
                right: Box::new(right.node),
                on,
            }),
            schema,
        })
    }

    fn bind_table_function(
        &self,
        name: String,
        args: Vec<parser::Expression>,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let catalog = self.catalog;
        let table_function = catalog.table_function(&name)?;
        let mut exprs = Vec::with_capacity(args.len());
        for (expr, column) in args.into_iter().zip(table_function.result_columns.iter()) {
            let expr = self
                .bind_expr(&PlanSchema::empty(), expr)?
                .expect_type(column.ty)?;
            exprs.push(expr);
        }
        let node = PlanNode::Project(planner::Project {
            source: Box::new(PlanNode::Values(Values::one_empty_row())),
            exprs,
        });
        let columns: Vec<_> = table_function.result_columns.clone();
        Ok(TypedPlanNode {
            node: PlanNode::Scan(planner::Scan::FunctionScan {
                source: Box::new(node),
                fn_ptr: table_function.fn_ptr,
            }),
            schema: columns.into(),
        })
    }

    fn bind_values(&self, values: parser::Values) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let mut rows = Vec::with_capacity(values.rows.len());
        let mut columns;
        let mut rows_iter = values.rows.into_iter();
        match rows_iter.next() {
            Some(row) => {
                let mut exprs = Vec::with_capacity(row.len());
                columns = Vec::with_capacity(row.len());
                let row = row.into_iter().enumerate();
                for (i, expr) in row {
                    let TypedExpression { expr, ty } =
                        self.bind_expr(&PlanSchema::empty(), expr)?;
                    exprs.push(expr);
                    columns.push(planner::Column {
                        table_name: None,
                        column_name: format!("column{}", i + 1),
                        ty,
                    });
                }
                rows.push(exprs);
            }
            None => return Ok(TypedPlanNode::empty_source()),
        }
        for row in rows_iter {
            assert_eq!(row.len(), columns.len());
            let mut exprs = Vec::with_capacity(row.len());
            for (expr, column) in row.into_iter().zip(columns.iter()) {
                let expr = self
                    .bind_expr(&PlanSchema::empty(), expr)?
                    .expect_type(column.ty)?;
                exprs.push(expr);
            }
            rows.push(exprs);
        }
        let node = PlanNode::Values(planner::Values { rows });
        Ok(TypedPlanNode {
            node,
            schema: columns.into(),
        })
    }

    #[allow(clippy::only_used_in_recursion)]
    fn bind_expr(
        &self,
        schema: &PlanSchema,
        expr: parser::Expression,
    ) -> PlannerResult<TypedExpression> {
        match expr {
            parser::Expression::Constant(value) => {
                let ty = value.ty();
                Ok(TypedExpression {
                    expr: planner::Expression::Constact(value),
                    ty,
                })
            }
            parser::Expression::ColumnRef(column_ref) => {
                let (index, column) = schema.resolve_column(&column_ref)?;
                let expr = planner::Expression::ColumnRef { index };
                Ok(TypedExpression {
                    expr,
                    ty: column.ty,
                })
            }
            parser::Expression::UnaryOp { op, expr } => {
                let TypedExpression { expr, ty } = self.bind_expr(schema, *expr)?;
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
                Ok(TypedExpression { expr, ty })
            }
            parser::Expression::BinaryOp { op, lhs, rhs } => {
                let lhs = self.bind_expr(schema, *lhs)?;
                let rhs = self.bind_expr(schema, *rhs)?;
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
                Ok(TypedExpression { expr, ty })
            }
        }
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

use super::{
    aggregate::AggregateContext,
    expression::{ExpressionBinder, TypedExpression},
    Explain, ExplainVisitor, Plan, PlanNode, Planner, PlannerResult,
};
use crate::{
    catalog::TableFnPtr,
    parser::{self, Distinct, NullOrder, Order, QueryModifier},
    planner,
    rows::ColumnIndex,
    storage::Table,
    types::NullableType,
    PlannerError, Storage, Type,
};
use std::fmt::Write;

pub struct Values<T: Storage> {
    pub rows: Vec<Vec<planner::Expression<T>>>,
}

impl<T: Storage> Values<T> {
    pub fn new(rows: Vec<Vec<planner::Expression<T>>>) -> Self {
        Self { rows }
    }

    pub fn one_empty_row() -> Self {
        Self::new(vec![Vec::new()])
    }
}

impl<T: Storage> Explain for Values<T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut f = "Values ".to_owned();
        for (i, row) in self.rows.iter().enumerate() {
            f.push_str(if i == 0 { "(" } else { ", (" });
            for (j, value) in row.iter().enumerate() {
                if j == 0 {
                    write!(&mut f, "{value}").unwrap();
                } else {
                    write!(&mut f, ", {value}").unwrap();
                }
            }
            f.push(')');
        }
        visitor.write_str(&f);
    }
}

pub enum Scan<'txn, 'db, T: Storage> {
    SeqScan {
        table: Table<'txn, 'db, T>,
    },
    FunctionScan {
        source: Box<PlanNode<'txn, 'db, T>>,
        fn_ptr: TableFnPtr<T>,
    },
}

impl<T: Storage> Explain for Scan<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        match self {
            Self::SeqScan { table } => {
                write!(visitor, "SeqScan table={:?}", table.name());
            }
            Self::FunctionScan { source, .. } => {
                visitor.write_str("FunctionScan");
                source.visit(visitor);
            }
        }
    }
}

pub struct Project<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub exprs: Vec<planner::Expression<T>>,
}

impl<T: Storage> Explain for Project<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut f = "Project ".to_owned();
        for (i, expr) in self.exprs.iter().enumerate() {
            if i == 0 {
                write!(f, "{expr}").unwrap();
            } else {
                write!(f, ", {expr}").unwrap();
            }
        }
        visitor.write_str(&f);
        self.source.visit(visitor);
    }
}

pub struct Filter<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub cond: planner::Expression<T>,
}

impl<T: Storage> Explain for Filter<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Filter {}", self.cond);
        self.source.visit(visitor);
    }
}

pub struct Sort<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub order_by: Vec<OrderBy<T>>,
}

impl<T: Storage> Explain for Sort<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut f = "Sort by [".to_owned();
        for (i, order_by) in self.order_by.iter().enumerate() {
            if i == 0 {
                write!(f, "{order_by}").unwrap();
            } else {
                write!(f, ", {order_by}").unwrap();
            }
        }
        f.push(']');
        visitor.write_str(&f);
        self.source.visit(visitor);
    }
}

pub struct Limit<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub limit: Option<planner::Expression<T>>,
    pub offset: Option<planner::Expression<T>>,
}

impl<T: Storage> Explain for Limit<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut f = "Limit".to_owned();
        if let Some(limit) = &self.limit {
            write!(f, " limit={limit}").unwrap();
        }
        if let Some(offset) = &self.offset {
            write!(f, " offset={offset}").unwrap();
        }
        visitor.write_str(&f);
        self.source.visit(visitor);
    }
}

pub struct OrderBy<T: Storage> {
    pub expr: planner::Expression<T>,
    pub order: Order,
    pub null_order: NullOrder,
}

impl<T: Storage> std::fmt::Display for OrderBy<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.expr, self.order, self.null_order)
    }
}

pub struct CrossProduct<'txn, 'db, T: Storage> {
    pub left: Box<PlanNode<'txn, 'db, T>>,
    pub right: Box<PlanNode<'txn, 'db, T>>,
}

impl<T: Storage> Explain for CrossProduct<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        visitor.write_str("CrossProduct");
        self.left.visit(visitor);
        self.right.visit(visitor);
    }
}

pub struct Union<'txn, 'db, T: Storage> {
    pub left: Box<PlanNode<'txn, 'db, T>>,
    pub right: Box<PlanNode<'txn, 'db, T>>,
}

impl<T: Storage> Explain for Union<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        visitor.write_str("Union");
        self.left.visit(visitor);
        self.right.visit(visitor);
    }
}

impl<'txn, 'db, T: Storage> Planner<'txn, 'db, T> {
    pub fn plan_query(&self, query: parser::Query) -> PlannerResult<Plan<'txn, 'db, T>> {
        match query.body {
            parser::QueryBody::Select(select) => self.plan_select(select, query.modifier),
            parser::QueryBody::Union { all, left, right } => {
                self.plan_union(all, *left, *right, query.modifier)
            }
        }
    }

    fn plan_select(
        &self,
        select: parser::Select,
        modifier: QueryModifier,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut plan = self.plan_table_ref(select.from)?;
        if let Some(where_clause) = select.where_clause {
            plan = self.plan_where_clause(plan, where_clause)?;
        }

        let mut aggregate_ctx = AggregateContext::default();

        // We need to determine whether the query is an aggregate query or not.
        // We search for all occurrences of aggregate functions in the query.
        if let Some(having) = &select.having {
            plan = aggregate_ctx.gather_aggregates(self, plan, having)?;
        }
        for projection in &select.projections {
            match projection {
                parser::Projection::Wildcard => (),
                parser::Projection::Expression { expr, .. } => {
                    plan = aggregate_ctx.gather_aggregates(self, plan, expr)?;
                }
            }
        }
        if let Some(Distinct { on: Some(on) }) = &select.distinct {
            for expr in on {
                plan = aggregate_ctx.gather_aggregates(self, plan, expr)?;
            }
        }
        for order_by in &modifier.order_by {
            plan = aggregate_ctx.gather_aggregates(self, plan, &order_by.expr)?;
        }

        // The query is an aggregate query if there are any aggregate functions
        // or if there is a GROUP BY clause.
        if aggregate_ctx.has_aggregates() || !select.group_by.is_empty() {
            plan = aggregate_ctx.plan_aggregates(self, plan, select.group_by)?;
        }

        // HAVING, ORDER BY and SELECT expressions can reference
        // the aggregated expressions. They are bound with the special
        // binder that allows them to reference the aggregated expressions.
        let aggregated_expr_binder =
            ExpressionBinder::new(self).with_aggregate_context(aggregate_ctx);

        if let Some(having) = select.having {
            plan = self.plan_having(&aggregated_expr_binder, plan, having)?;
        }

        // FIXME: order_by should be after projections according to SQL
        //        semantics. However, if projections reorder columns,
        //        we lose track of aggregation results. So we process order_by
        //        before projections for now.
        let plan = self.plan_order_by(&aggregated_expr_binder, plan, modifier.order_by)?;
        let plan = self.plan_projections(
            &aggregated_expr_binder,
            plan,
            select.projections,
            select.distinct,
        )?;
        let plan = self.plan_limit(plan, modifier.limit, modifier.offset)?;
        Ok(plan)
    }

    fn plan_union(
        &self,
        all: bool,
        left: parser::Query,
        right: parser::Query,
        modifier: QueryModifier,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let Plan {
            node: left_node,
            schema: left_schema,
        } = self.plan_query(left)?;
        let Plan {
            node: right_node,
            schema: right_schema,
        } = self.plan_query(right)?;
        if right_schema.0.len() != left_schema.0.len() {
            return Err(PlannerError::ColumnCountMismatch {
                expected: left_schema.0.len(),
                actual: right_schema.0.len(),
            });
        }
        let mut exprs = Vec::with_capacity(right_schema.0.len());
        for (i, (left_column, right_column)) in
            left_schema.0.iter().zip(&right_schema.0).enumerate()
        {
            let mut expr = planner::Expression::ColumnRef(ColumnIndex(i));
            if let NullableType::NonNull(left_type) = left_column.ty {
                if !right_column.ty.is_compatible_with(left_type) {
                    if !right_column.ty.can_cast_to(left_type) {
                        return Err(PlannerError::TypeError);
                    }
                    expr = planner::Expression::Cast {
                        expr: Box::new(expr),
                        ty: left_type,
                    }
                }
            };
            exprs.push(expr);
        }
        let right_node = PlanNode::Project(planner::Project {
            source: Box::new(right_node),
            exprs,
        });
        let mut node = PlanNode::Union(planner::Union {
            left: Box::new(left_node),
            right: Box::new(right_node),
        });
        if !all {
            node = PlanNode::Aggregate(planner::Aggregate::Hash {
                source: Box::new(node),
                column_ops: left_schema
                    .0
                    .iter()
                    .map(|_| planner::AggregateOp::GroupBy)
                    .collect(),
            });
        }
        let plan = Plan {
            node,
            schema: left_schema,
        };
        let expr_binder = ExpressionBinder::new(self);
        let plan = self.plan_order_by(&expr_binder, plan, modifier.order_by)?;
        let plan = self.plan_limit(plan, modifier.limit, modifier.offset)?;
        Ok(plan)
    }

    pub fn plan_where_clause(
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
    fn plan_having(
        &self,
        expr_binder: &ExpressionBinder<'_, 'txn, 'db, T>,
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
    fn plan_projections(
        &self,
        expr_binder: &ExpressionBinder<'_, 'txn, 'db, T>,
        source: Plan<'txn, 'db, T>,
        projections: Vec<parser::Projection>,
        distinct: Option<Distinct>,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let mut plan = source;
        let mut exprs = Vec::new();
        let mut columns = Vec::new();
        for projection in projections {
            match projection {
                parser::Projection::Wildcard => {
                    for (i, column) in plan.schema.0.iter().cloned().enumerate() {
                        exprs.push(planner::Expression::ColumnRef(ColumnIndex(i)));
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
                    } else if let planner::Expression::ColumnRef(index) = &bound_expr.expr {
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
        let node = match distinct {
            Some(Distinct { on: Some(on) }) => {
                let column_ops = (0..columns.len())
                    .map(|_| planner::AggregateOp::Passthrough)
                    .chain((0..on.len()).map(|_| planner::AggregateOp::GroupBy))
                    .collect();
                for expr in on {
                    let (new_plan, TypedExpression { expr, .. }) = expr_binder.bind(plan, expr)?;
                    plan = new_plan;
                    exprs.push(expr);
                }
                let node = PlanNode::Project(planner::Project {
                    source: Box::new(plan.node),
                    exprs,
                });
                let node = PlanNode::Aggregate(planner::Aggregate::Hash {
                    source: Box::new(node),
                    column_ops,
                });
                PlanNode::Project(planner::Project {
                    source: Box::new(node),
                    exprs: (0..columns.len())
                        .map(|i| planner::Expression::ColumnRef(ColumnIndex(i)))
                        .collect(),
                })
            }
            Some(Distinct { on: None }) => {
                let node = PlanNode::Project(planner::Project {
                    source: Box::new(plan.node),
                    exprs,
                });
                PlanNode::Aggregate(planner::Aggregate::Hash {
                    source: Box::new(node),
                    column_ops: columns
                        .iter()
                        .map(|_| planner::AggregateOp::GroupBy)
                        .collect(),
                })
            }
            None => PlanNode::Project(planner::Project {
                source: Box::new(plan.node),
                exprs,
            }),
        };
        Ok(Plan {
            node,
            schema: columns.into(),
        })
    }

    #[allow(clippy::unused_self)]
    fn plan_order_by(
        &self,
        expr_binder: &ExpressionBinder<'_, 'txn, 'db, T>,
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

    fn plan_limit(
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
    ) -> PlannerResult<Option<planner::Expression<T>>> {
        let Some(expr) = expr else {
            return Ok(None);
        };
        let expr = self
            .bind_expr_without_source(expr)?
            .expect_type(Type::Integer)?;
        Ok(Some(expr))
    }

    fn plan_table_ref(&self, table_ref: parser::TableRef) -> PlannerResult<Plan<'txn, 'db, T>> {
        match table_ref {
            parser::TableRef::BaseTable { name } => {
                Ok(self.plan_base_table(self.catalog.table(name)?))
            }
            parser::TableRef::Join(join) => self.plan_join(*join),
            parser::TableRef::Subquery(query) => self.plan_query(*query),
            parser::TableRef::Function { name, args } => self.plan_table_function(name, args),
            parser::TableRef::Values(values) => self.plan_values(values),
        }
    }

    #[allow(clippy::unused_self)]
    pub fn plan_base_table(&self, table: Table<'txn, 'db, T>) -> Plan<'txn, 'db, T> {
        let columns: Vec<_> = table
            .columns()
            .iter()
            .cloned()
            .map(|column| planner::Column {
                table_name: Some(table.name().to_owned()),
                column_name: column.name,
                ty: column.ty.into(),
            })
            .collect();
        Plan {
            node: PlanNode::Scan(planner::Scan::SeqScan { table }),
            schema: columns.into(),
        }
    }

    fn plan_join(&self, join: parser::Join) -> PlannerResult<Plan<'txn, 'db, T>> {
        let left = self.plan_table_ref(join.left)?;
        let right = self.plan_table_ref(join.right)?;
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

    fn plan_table_function(
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

    fn plan_values(&self, values: parser::Values) -> PlannerResult<Plan<'txn, 'db, T>> {
        if values.rows.is_empty() {
            return Ok(Plan::empty_source());
        }

        let mut rows = Vec::with_capacity(values.rows.len());
        let num_columns = values.rows[0].len();
        let mut column_types = vec![NullableType::Null; num_columns];
        for row in values.rows {
            assert_eq!(row.len(), num_columns);
            let mut exprs = Vec::with_capacity(num_columns);
            for (expr, column_type) in row.into_iter().zip(column_types.iter_mut()) {
                let TypedExpression { expr, ty } = self.bind_expr_without_source(expr)?;
                if !ty.is_compatible_with(*column_type) {
                    return Err(PlannerError::TypeError);
                }
                if matches!(column_type, NullableType::Null) {
                    *column_type = ty;
                }
                exprs.push(expr);
            }
            rows.push(exprs);
        }

        let columns: Vec<_> = column_types
            .into_iter()
            .enumerate()
            .map(|(i, ty)| planner::Column {
                table_name: None,
                column_name: format!("column{}", i + 1),
                ty,
            })
            .collect();

        let node = PlanNode::Values(planner::Values { rows });
        Ok(Plan {
            node,
            schema: columns.into(),
        })
    }
}

use super::{
    aggregate::AggregateCollection,
    expression::{ExpressionBinder, TypedExpression},
    Column, ColumnId, ColumnMap, ExplainFormatter, Node, PlanNode, Planner, PlannerResult,
};
use crate::{
    catalog::TableFunction,
    parser::{self, NullOrder, Order},
    planner,
    rows::ColumnIndex,
    storage::{Table, Transaction},
    types::NullableType,
    PlannerError, Type,
};
use std::{collections::HashMap, fmt::Write};

pub struct Values<'a, T> {
    pub rows: Vec<Vec<planner::Expression<'a, T, ColumnId>>>,
    outputs: Vec<ColumnId>,
}

impl<'a, T> Values<'a, T> {
    pub fn one_empty_row() -> Self {
        Self {
            rows: vec![Vec::new()],
            outputs: Vec::new(),
        }
    }
}

impl<T> Node for Values<'_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.write_str("Values");
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.extend(self.outputs.iter());
    }
}

pub enum Scan<'a, T> {
    SeqScan {
        table: Table<'a, T>,
        outputs: Vec<ColumnId>,
    },
    FunctionScan {
        source: Box<PlanNode<'a, T>>,
        function: &'a TableFunction<T>,
        outputs: Vec<ColumnId>,
    },
}

impl<T> Node for Scan<'_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        match self {
            Self::SeqScan { table, .. } => {
                write!(f, "SeqScan on {}", table.name());
            }
            Self::FunctionScan {
                source, function, ..
            } => {
                write!(f, "FunctionScan on {}", function.name);
                source.fmt_explain(f);
            }
        }
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        match self {
            Self::SeqScan { outputs, .. } | Self::FunctionScan { outputs, .. } => {
                columns.extend(outputs.iter());
            }
        }
    }
}

pub struct Project<'a, T> {
    pub source: Box<PlanNode<'a, T>>,
    pub outputs: Vec<(ColumnId, planner::Expression<'a, T, ColumnId>)>,
}

impl<T> Node for Project<'_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        let mut s = "Project ".to_owned();
        for (i, (output, expr)) in self.outputs.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            write!(s, "{expr} -> {output}").unwrap();
        }
        f.write_str(&s);
        self.source.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.extend(self.outputs.iter().map(|(id, _)| *id));
    }
}

pub struct Filter<'a, T> {
    pub source: Box<PlanNode<'a, T>>,
    pub condition: planner::Expression<'a, T, ColumnId>,
}

impl<T> Node for Filter<'_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        write!(f, "Filter {}", self.condition);
        self.source.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

pub struct Sort<'a, T> {
    pub source: Box<PlanNode<'a, T>>,
    pub order_by: Vec<OrderBy<'a, T, ColumnId>>,
}

impl<T> Node for Sort<'_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        let mut s = "Sort by ".to_owned();
        for (i, order_by) in self.order_by.iter().enumerate() {
            if i == 0 {
                write!(s, "{order_by}").unwrap();
            } else {
                write!(s, ", {order_by}").unwrap();
            }
        }
        f.write_str(&s);
        self.source.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

pub struct Limit<'a, T> {
    pub source: Box<PlanNode<'a, T>>,
    pub limit: Option<planner::Expression<'a, T, ColumnId>>,
    pub offset: Option<planner::Expression<'a, T, ColumnId>>,
}

impl<T> Node for Limit<'_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.write_str("Limit");
        self.source.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

pub struct OrderBy<'a, T, C> {
    pub expr: planner::Expression<'a, T, C>,
    pub order: Order,
    pub null_order: NullOrder,
}

impl<T, C: std::fmt::Display> std::fmt::Display for OrderBy<'_, T, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.expr)?;
        if self.order != Default::default() {
            write!(f, " {}", self.order)?;
        }
        if self.null_order != Default::default() {
            write!(f, " {}", self.null_order)?;
        }
        Ok(())
    }
}

impl<'a, T> OrderBy<'a, T, ColumnId> {
    pub fn into_executable(self, columns: &[ColumnId]) -> OrderBy<'a, T, ColumnIndex> {
        OrderBy {
            expr: self.expr.into_executable(columns),
            order: self.order,
            null_order: self.null_order,
        }
    }
}

pub struct CrossProduct<'a, T> {
    pub left: Box<PlanNode<'a, T>>,
    pub right: Box<PlanNode<'a, T>>,
}

impl<T> Node for CrossProduct<'_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.write_str("CrossProduct");
        self.left.fmt_explain(f);
        self.right.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.left.append_outputs(columns);
        self.right.append_outputs(columns);
    }
}

pub struct Union<'a, T> {
    pub left: Box<PlanNode<'a, T>>,
    pub right: Box<PlanNode<'a, T>>,
    outputs: Vec<ColumnId>,
}

impl<T> Node for Union<'_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.write_str("Union");
        self.left.fmt_explain(f);
        self.right.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.extend(self.outputs.iter());
    }
}

impl<'a, T> PlanNode<'a, T> {
    fn new_values(
        column_map: &mut ColumnMap,
        rows: Vec<Vec<planner::Expression<'a, T, ColumnId>>>,
        column_types: Vec<NullableType>,
    ) -> Self {
        if rows.is_empty() {
            return Self::new_empty_values();
        }
        let outputs = column_types
            .into_iter()
            .enumerate()
            .map(|(i, ty)| column_map.insert(Column::new(format!("column{}", i + 1), ty)))
            .collect();
        Self::Values(Values { rows, outputs })
    }

    pub(super) fn new_empty_values() -> Self {
        Self::Values(Values::one_empty_row())
    }

    fn function_scan(self, column_map: &mut ColumnMap, function: &'a TableFunction<T>) -> Self {
        let outputs = function
            .result_columns
            .iter()
            .map(|column| column_map.insert(column.clone()))
            .collect();
        Self::Scan(Scan::FunctionScan {
            source: Box::new(self),
            function,
            outputs,
        })
    }

    pub(super) fn project(
        self,
        column_map: &mut ColumnMap,
        exprs: Vec<TypedExpression<'a, T>>,
    ) -> Self {
        let outputs = exprs
            .into_iter()
            .map(|expr| {
                let TypedExpression { expr, ty } = expr;
                let id = match expr {
                    planner::Expression::ColumnRef(id) => id,
                    _ => column_map.insert(Column::new(expr.to_string(), ty)),
                };
                (id, expr)
            })
            .collect();
        Self::Project(Project {
            source: Box::new(self),
            outputs,
        })
    }

    fn filter(self, condition: TypedExpression<'a, T>) -> PlannerResult<Self> {
        Ok(Self::Filter(Filter {
            source: Box::new(self),
            condition: condition.expect_type(Type::Boolean)?,
        }))
    }

    fn sort(self, order_by: Vec<OrderBy<'a, T, ColumnId>>) -> Self {
        Self::Sort(Sort {
            source: Box::new(self),
            order_by,
        })
    }

    pub(super) fn limit(
        self,
        limit: Option<TypedExpression<'a, T>>,
        offset: Option<TypedExpression<'a, T>>,
    ) -> PlannerResult<Self> {
        if limit.is_none() && offset.is_none() {
            return Ok(self);
        }
        let limit = limit
            .map(|limit| limit.expect_type(Type::Integer))
            .transpose()?;
        let offset = offset
            .map(|offset| offset.expect_type(Type::Integer))
            .transpose()?;
        Ok(Self::Limit(Limit {
            source: Box::new(self),
            limit,
            offset,
        }))
    }

    pub(super) fn cross_product(self, other: Self) -> Self {
        Self::CrossProduct(CrossProduct {
            left: Box::new(self),
            right: Box::new(other),
        })
    }

    fn union(self, column_map: &mut ColumnMap, other: Self) -> PlannerResult<Self> {
        let left_outputs = self.outputs();
        let right_outputs = other.outputs();
        if left_outputs.len() != right_outputs.len() {
            return Err(PlannerError::ColumnCountMismatch {
                expected: left_outputs.len(),
                actual: right_outputs.len(),
            });
        }
        for (left, right) in left_outputs.iter().zip(right_outputs.iter()) {
            if !column_map[*left]
                .ty
                .is_compatible_with(column_map[*right].ty)
            {
                return Err(PlannerError::TypeError);
            }
        }
        let outputs = self
            .outputs()
            .into_iter()
            .map(|id| column_map.insert(column_map[id].clone()))
            .collect();
        Ok(Self::Union(Union {
            left: Box::new(self),
            right: Box::new(other),
            outputs,
        }))
    }
}

impl<'a, T: Transaction> PlanNode<'a, T> {
    fn new_seq_scan(column_map: &mut ColumnMap, table: Table<'a, T>) -> Self {
        let outputs = table
            .columns()
            .iter()
            .map(|column| {
                column_map.insert(Column {
                    table_name: Some(table.name().to_owned()),
                    column_name: column.name.clone(),
                    ty: column.ty.into(),
                })
            })
            .collect();
        Self::Scan(Scan::SeqScan { table, outputs })
    }
}

impl<'a, T: Transaction> Planner<'a, T> {
    pub fn plan_query(&self, query: parser::Query) -> PlannerResult<PlanNode<'a, T>> {
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
        modifier: parser::QueryModifier,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let mut plan = self.plan_table_ref(&ExpressionBinder::new(self), select.from)?;

        // Any occurrences of aggregate functions in SELECT, HAVING and ORDER BY
        // clauses make the query an aggregate query.
        // So we first gather all occurrences of aggregate functions into
        // aggregate_collection to determine if the query is an aggregate query.
        let mut aggregate_collection = AggregateCollection::new(self);
        for projection in &select.projections {
            match projection {
                parser::Projection::Wildcard => (),
                parser::Projection::Expression { expr, .. } => {
                    plan = aggregate_collection.gather(plan, expr)?;
                }
            }
        }
        if let Some(parser::Distinct { on: Some(on) }) = &select.distinct {
            for expr in on {
                plan = aggregate_collection.gather(plan, expr)?;
            }
        }
        if let Some(having) = &select.having {
            plan = aggregate_collection.gather(plan, having)?;
        }
        for order_by in &modifier.order_by {
            plan = aggregate_collection.gather(plan, &order_by.expr)?;
        }
        let mut aggregate_planner = aggregate_collection.finish();

        // Next, we expand * and resolve aliases in SELECT clause.
        let mut projection_exprs = Vec::new();
        let mut aliases = HashMap::new();
        let outputs = plan.outputs();
        for projection in select.projections {
            match projection {
                parser::Projection::Wildcard => {
                    let column_map = self.column_map();
                    for output in &outputs {
                        let column = &column_map[*output];
                        projection_exprs.push(parser::Expression::ColumnRef(parser::ColumnRef {
                            table_name: column.table_name.clone(),
                            column_name: column.column_name.clone(),
                        }));
                    }
                }
                parser::Projection::Expression { expr, alias } => {
                    let Some(alias) = alias else {
                        projection_exprs.push(expr);
                        continue;
                    };

                    let expr_binder = ExpressionBinder::new(self)
                        .with_aliases(&aliases)
                        .with_aggregates(&aggregate_planner);
                    let (new_plan, expr) = expr_binder.bind(plan, expr)?;
                    plan = new_plan;

                    // Adding aliases one by one makes sure that aliases that
                    // appear later in the SELECT clause can only refer to
                    // aliases that appear earlier, preventing
                    // circular references.
                    // Aliases with the same name shadow previous aliases.
                    aliases.insert(alias.clone(), expr);

                    projection_exprs.push(parser::Expression::ColumnRef(parser::ColumnRef {
                        table_name: None,
                        column_name: alias,
                    }));
                }
            }
        }

        // Now we are ready to process the rest of the clauses with
        // the aliases and aggregate results.

        // WHERE and GROUP BY can refer to aliases but not aggregate results.
        let expr_binder = ExpressionBinder::new(self).with_aliases(&aliases);

        if let Some(where_clause) = select.where_clause {
            plan = self.plan_filter(&expr_binder, plan, where_clause)?;
        }

        // The query is an aggregate query if there are any aggregate functions
        // or if there is a GROUP BY clause.
        if aggregate_planner.has_aggregates() || !select.group_by.is_empty() {
            plan = aggregate_planner.plan(&expr_binder, plan, select.group_by)?;
        }

        // SELECT, HAVING and ORDER BY can refer to both aliases and
        // aggregate results.
        let expr_binder = ExpressionBinder::new(self)
            .with_aliases(&aliases)
            .with_aggregates(&aggregate_planner);

        if let Some(having) = select.having {
            plan = self.plan_filter(&expr_binder, plan, having)?;
        }

        // FIXME: order_by should be after projections according to SQL
        //        semantics. However, if projections reorder columns,
        //        we lose track of aggregation results. So we process order_by
        //        before projections for now.
        let plan = self.plan_order_by(&expr_binder, plan, modifier.order_by)?;
        let plan = self.plan_projections(&expr_binder, plan, projection_exprs, select.distinct)?;

        self.plan_limit(
            &ExpressionBinder::new(self),
            plan,
            modifier.limit,
            modifier.offset,
        )
    }

    fn plan_union(
        &self,
        all: bool,
        left: parser::Query,
        right: parser::Query,
        modifier: parser::QueryModifier,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let left = self.plan_query(left)?;
        let right = self.plan_query(right)?;
        let mut plan = left.union(&mut self.column_map(), right)?;
        if !all {
            let ops = plan
                .outputs()
                .into_iter()
                .map(|target| planner::AggregateOp::GroupBy { target })
                .collect();
            plan = plan.hash_aggregate(ops);
        }
        let expr_binder = ExpressionBinder::new(self);
        let plan = self.plan_order_by(&expr_binder, plan, modifier.order_by)?;
        self.plan_limit(&expr_binder, plan, modifier.limit, modifier.offset)
    }

    #[allow(clippy::unused_self)]
    pub fn plan_filter(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        source: PlanNode<'a, T>,
        expr: parser::Expression,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let (plan, condition) = expr_binder.bind(source, expr)?;
        plan.filter(condition)
    }

    fn plan_projections(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        source: PlanNode<'a, T>,
        projection_exprs: Vec<parser::Expression>,
        distinct: Option<parser::Distinct>,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let num_projected_columns = projection_exprs.len();

        let mut plan = source;
        let mut exprs = Vec::new();
        for expr in projection_exprs {
            let (new_plan, bound_expr) = expr_binder.bind(plan, expr.clone())?;
            plan = new_plan;
            exprs.push(bound_expr);
        }

        match distinct {
            Some(parser::Distinct { on: Some(on) }) => {
                for expr in on {
                    let (new_plan, expr) = expr_binder.bind(plan, expr)?;
                    plan = new_plan;
                    exprs.push(expr);
                }

                let mut column_map = self.column_map();
                let plan = plan.project(&mut column_map, exprs);
                let outputs = plan.outputs();

                let projected = outputs
                    .iter()
                    .take(num_projected_columns)
                    .map(|target| planner::AggregateOp::Passthrough { target: *target });
                let on = outputs
                    .iter()
                    .skip(num_projected_columns)
                    .map(|target| planner::AggregateOp::GroupBy { target: *target });
                let ops = projected.chain(on).collect();
                let plan = plan.hash_aggregate(ops);

                let exprs = outputs
                    .into_iter()
                    .take(num_projected_columns)
                    .map(|id| TypedExpression {
                        expr: planner::Expression::ColumnRef(id),
                        ty: column_map[id].ty,
                    })
                    .collect();
                Ok(plan.project(&mut column_map, exprs))
            }
            Some(parser::Distinct { on: None }) => {
                let plan = plan.project(&mut self.column_map(), exprs);
                let ops = plan
                    .outputs()
                    .into_iter()
                    .map(|target| planner::AggregateOp::GroupBy { target })
                    .collect();
                Ok(plan.hash_aggregate(ops))
            }
            None => Ok(plan.project(&mut self.column_map(), exprs)),
        }
    }

    #[allow(clippy::unused_self)]
    fn plan_order_by(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        source: PlanNode<'a, T>,
        order_by: Vec<parser::OrderBy>,
    ) -> PlannerResult<PlanNode<'a, T>> {
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
        Ok(plan.sort(bound_order_by))
    }

    #[allow(clippy::unused_self)]
    fn plan_limit(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        source: PlanNode<'a, T>,
        limit: Option<parser::Expression>,
        offset: Option<parser::Expression>,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let limit = limit
            .map(|expr| expr_binder.bind_without_source(expr))
            .transpose()?;
        let offset = offset
            .map(|expr| expr_binder.bind_without_source(expr))
            .transpose()?;
        source.limit(limit, offset)
    }

    fn plan_table_ref(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        table_ref: parser::TableRef,
    ) -> PlannerResult<PlanNode<'a, T>> {
        match table_ref {
            parser::TableRef::BaseTable { name } => {
                Ok(self.plan_base_table(self.ctx.catalog().table(name)?))
            }
            parser::TableRef::Join(join) => self.plan_join(expr_binder, *join),
            parser::TableRef::Subquery(query) => self.plan_query(*query),
            parser::TableRef::Function { name, args } => {
                self.plan_table_function(expr_binder, name, args)
            }
            parser::TableRef::Values(values) => self.plan_values(expr_binder, values),
        }
    }

    pub fn plan_base_table(&self, table: Table<'a, T>) -> PlanNode<'a, T> {
        PlanNode::new_seq_scan(&mut self.column_map(), table)
    }

    fn plan_join(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        join: parser::Join,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let left = self.plan_table_ref(expr_binder, join.left)?;
        let right = self.plan_table_ref(expr_binder, join.right)?;
        let plan = left.cross_product(right);
        let Some(on) = join.on else {
            return Ok(plan);
        };
        let (plan, on) = expr_binder.bind(plan, on)?;
        plan.filter(on)
    }

    fn plan_table_function(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        name: String,
        args: Vec<parser::Expression>,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let function = self.ctx.catalog().table_function(&name)?;
        let exprs = args
            .into_iter()
            .map(|expr| expr_binder.bind_without_source(expr))
            .collect::<PlannerResult<_>>()?;
        let mut column_map = self.column_map();
        Ok(PlanNode::new_empty_values()
            .project(&mut column_map, exprs)
            .function_scan(&mut column_map, function))
    }

    fn plan_values(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a, T>,
        values: parser::Values,
    ) -> PlannerResult<PlanNode<'a, T>> {
        if values.rows.is_empty() {
            return Ok(PlanNode::new_empty_values());
        }

        let mut rows = Vec::with_capacity(values.rows.len());
        let num_columns = values.rows[0].len();
        let mut column_types = vec![NullableType::Null; num_columns];
        for row in values.rows {
            assert_eq!(row.len(), num_columns);
            let mut exprs = Vec::with_capacity(num_columns);
            for (expr, column_type) in row.into_iter().zip(column_types.iter_mut()) {
                let TypedExpression { expr, ty } = expr_binder.bind_without_source(expr)?;
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

        Ok(PlanNode::new_values(
            &mut self.column_map(),
            rows,
            column_types,
        ))
    }
}

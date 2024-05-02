use super::{
    explain::ExplainFormatter,
    expression::{ExpressionBinder, PlanExpression, TypedExpression},
    ColumnId, ColumnMap, Node, PlanNode, Planner, PlannerResult,
};
use crate::{
    parser::{self, NullOrder, Order},
    planner,
    rows::ColumnIndex,
    Value,
};
use std::borrow::Cow;

#[derive(Clone)]
pub struct Sort<'a> {
    pub source: Box<PlanNode<'a>>,
    pub order_by: Vec<PlanOrderBy<'a>>,
}

impl Node for Sort<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Sort");
        for order_by in &self.order_by {
            node.field("key", order_by.clone().display(&f.column_map()));
        }
        node.child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }

    fn num_rows(&self) -> usize {
        self.source.num_rows()
    }

    fn cost(&self) -> f64 {
        let num_rows = self.source.num_rows() as f64;
        // Assume O(n log n) sorting algorithm.
        let sort_cost = num_rows * num_rows.log2().max(0.0) * PlanNode::DEFAULT_ROW_COST;
        self.source.cost() + sort_cost
    }
}

#[derive(Clone)]
pub struct TopN<'a> {
    pub source: Box<PlanNode<'a>>,
    pub limit: PlanExpression<'a>,
    pub offset: Option<PlanExpression<'a>>,
    pub order_by: Vec<PlanOrderBy<'a>>,
}

impl Node for TopN<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("TopN");
        let column_map = f.column_map();
        node.field("limit", self.limit.display(&column_map));
        if let Some(offset) = &self.offset {
            node.field("offset", offset.display(&column_map));
        }
        for order_by in &self.order_by {
            node.field("key", order_by.display(&column_map));
        }
        node.child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }

    fn num_rows(&self) -> usize {
        let mut num_rows = self.source.num_rows();
        if let Ok(Value::Integer(limit)) = self.limit.eval_const() {
            num_rows = num_rows.min(limit as usize);
        }
        num_rows
    }

    fn cost(&self) -> f64 {
        let source_num_rows = self.source.num_rows() as f64;
        let num_rows = self.num_rows() as f64;

        let mut heap_size = num_rows;
        if let Some(Ok(Value::Integer(offset))) =
            self.offset.as_ref().map(PlanExpression::eval_const)
        {
            heap_size += offset as f64;
        }
        let num_overflows = (source_num_rows - heap_size).max(0.0);

        // Insertion into a binary heap is O(1) on average.
        let insertion_cost = source_num_rows * PlanNode::DEFAULT_ROW_COST;

        // Deletion from a binary heap is O(log n) on average.
        let row_deletion_cost = heap_size.log2().max(0.0) * PlanNode::DEFAULT_ROW_COST;
        let overflow_cost = num_overflows * row_deletion_cost;
        let extraction_cost = num_rows * row_deletion_cost;

        self.source.cost() + insertion_cost + overflow_cost + extraction_cost
    }
}

pub type PlanOrderBy<'a> = OrderBy<'a, ColumnId>;
pub type ExecutableOrderBy<'a> = OrderBy<'a, ColumnIndex>;
pub type OrderByDisplay<'a, 'b> = OrderBy<'a, Cow<'b, str>>;

#[derive(Clone)]
pub struct OrderBy<'a, C> {
    pub expr: planner::Expression<'a, C>,
    pub order: Order,
    pub null_order: NullOrder,
}

impl std::fmt::Display for OrderByDisplay<'_, '_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.expr.fmt(f)?;
        if self.order != Default::default() {
            write!(f, " {}", self.order)?;
        }
        if self.null_order != self.order.default_null_order() {
            write!(f, " {}", self.null_order)?;
        }
        Ok(())
    }
}

impl<'a> PlanOrderBy<'a> {
    pub fn into_executable(self, columns: &[ColumnId]) -> ExecutableOrderBy<'a> {
        ExecutableOrderBy {
            expr: self.expr.into_executable(columns),
            order: self.order,
            null_order: self.null_order,
        }
    }

    pub(super) fn display<'b>(&self, column_map: &'b ColumnMap) -> OrderByDisplay<'a, 'b> {
        OrderByDisplay {
            expr: self.expr.display(column_map),
            order: self.order,
            null_order: self.null_order,
        }
    }
}

impl<'a> PlanNode<'a> {
    fn sort(self, order_by: Vec<PlanOrderBy<'a>>) -> Self {
        if self.produces_no_rows() {
            return self;
        }
        Self::Sort(Sort {
            source: Box::new(self),
            order_by,
        })
    }
}

impl<'a> Planner<'a> {
    #[allow(clippy::unused_self)]
    pub fn plan_order_by(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        source: PlanNode<'a>,
        order_by: Vec<parser::OrderBy>,
    ) -> PlannerResult<PlanNode<'a>> {
        if order_by.is_empty() {
            return Ok(source);
        }
        let mut plan = source;
        let mut bound_order_by = Vec::with_capacity(order_by.len());
        for item in order_by {
            let (new_plan, TypedExpression { expr, .. }) = expr_binder.bind(plan, item.expr)?;
            plan = new_plan;
            bound_order_by.push(PlanOrderBy {
                expr,
                order: item.order,
                null_order: item.null_order,
            });
        }
        Ok(plan.sort(bound_order_by))
    }
}

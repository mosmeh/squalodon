use super::{
    column::ColumnMapView,
    explain::ExplainFormatter,
    expression::{ExpressionBinder, TypedExpression},
    ColumnId, Node, PlanNode, Planner, PlannerResult,
};
use crate::{
    parser::{self, NullOrder, Order},
    planner,
    rows::ColumnIndex,
};
use std::borrow::Cow;

pub struct Sort<'a> {
    pub source: Box<PlanNode<'a>>,
    pub order_by: Vec<OrderBy<'a, ColumnId>>,
}

impl Node for Sort<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Sort");
        for order_by in &self.order_by {
            node.field("key", order_by.clone().into_display(&f.column_map()));
        }
        node.child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

pub struct TopN<'a> {
    pub source: Box<PlanNode<'a>>,
    pub limit: planner::Expression<'a, ColumnId>,
    pub offset: Option<planner::Expression<'a, ColumnId>>,
    pub order_by: Vec<OrderBy<'a, ColumnId>>,
}

impl Node for TopN<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("TopN");
        node.field("limit", self.limit.clone().into_display(&f.column_map()));
        if let Some(offset) = &self.offset {
            node.field("offset", offset.clone().into_display(&f.column_map()));
        }
        for order_by in &self.order_by {
            node.field("key", order_by.clone().into_display(&f.column_map()));
        }
        node.child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.source.append_outputs(columns);
    }
}

#[derive(Clone)]
pub struct OrderBy<'a, C> {
    pub expr: planner::Expression<'a, C>,
    pub order: Order,
    pub null_order: NullOrder,
}

impl std::fmt::Display for OrderBy<'_, Cow<'_, str>> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.expr.fmt(f)?;
        if self.order != Default::default() {
            write!(f, " {}", self.order)?;
        }
        if self.null_order != Default::default() {
            write!(f, " {}", self.null_order)?;
        }
        Ok(())
    }
}

impl<'a> OrderBy<'a, ColumnId> {
    pub fn into_executable(self, columns: &[ColumnId]) -> OrderBy<'a, ColumnIndex> {
        OrderBy {
            expr: self.expr.into_executable(columns),
            order: self.order,
            null_order: self.null_order,
        }
    }

    pub(super) fn into_display<'b>(
        self,
        column_map: &'b ColumnMapView,
    ) -> OrderBy<'a, Cow<'b, str>> {
        OrderBy {
            expr: self.expr.into_display(column_map),
            order: self.order,
            null_order: self.null_order,
        }
    }
}

impl<'a> PlanNode<'a> {
    fn sort(self, order_by: Vec<OrderBy<'a, ColumnId>>) -> Self {
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
            bound_order_by.push(planner::OrderBy {
                expr,
                order: item.order,
                null_order: item.null_order,
            });
        }
        Ok(plan.sort(bound_order_by))
    }
}

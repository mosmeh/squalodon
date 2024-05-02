use super::{
    explain::ExplainFormatter,
    expression::{ExpressionBinder, PlanExpression, TypedExpression},
    ColumnId, Filter, Node, PlanNode, Planner, PlannerResult,
};
use crate::{
    parser::{self, BinaryOp},
    Value,
};

#[derive(Clone)]
pub struct CrossProduct<'a> {
    pub left: Box<PlanNode<'a>>,
    pub right: Box<PlanNode<'a>>,
}

impl Node for CrossProduct<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("CrossProduct").child(&self.left).child(&self.right);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        self.left.append_outputs(columns);
        self.right.append_outputs(columns);
    }

    fn num_rows(&self) -> usize {
        self.left.num_rows() * self.right.num_rows()
    }

    fn cost(&self) -> f64 {
        let cross_product_cost =
            self.left.num_rows() as f64 * self.right.num_rows() as f64 * PlanNode::DEFAULT_ROW_COST;
        self.left.cost() + self.right.cost() + cross_product_cost
    }
}

#[derive(Clone)]
pub enum Join<'a> {
    NestedLoop {
        left: Box<PlanNode<'a>>,
        right: Box<PlanNode<'a>>,
        comparisons: Vec<(CompareOp, PlanExpression<'a>, PlanExpression<'a>)>,
    },
    Hash {
        left: Box<PlanNode<'a>>,
        right: Box<PlanNode<'a>>,
        keys: Vec<(PlanExpression<'a>, PlanExpression<'a>)>,
    },
}

impl Node for Join<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        match self {
            Self::NestedLoop {
                left,
                right,
                comparisons,
            } => {
                let mut node = f.node("NestedLoopJoin");
                let column_map = f.column_map();
                for (op, left_key, right_key) in comparisons {
                    let left_key = left_key.display(&column_map);
                    let right_key = right_key.display(&column_map);
                    node.field("condition", format!("{left_key} {op} {right_key}"));
                }
                node.child(left).child(right);
            }
            Self::Hash { left, right, keys } => {
                let mut node = f.node("HashJoin");
                let column_map = f.column_map();
                for (left_key, right_key) in keys {
                    let left_key = left_key.display(&column_map);
                    let right_key = right_key.display(&column_map);
                    node.field("condition", format!("{left_key} = {right_key}"));
                }
                node.child(left).child(right);
            }
        }
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        match self {
            Self::NestedLoop { left, right, .. } | Self::Hash { left, right, .. } => {
                left.append_outputs(columns);
                right.append_outputs(columns);
            }
        }
    }

    fn num_rows(&self) -> usize {
        match self {
            Self::NestedLoop {
                left,
                right,
                comparisons,
            } => {
                let mut num_rows = left.num_rows() as f64 * right.num_rows() as f64;
                for _ in comparisons {
                    num_rows *= Filter::DEFAULT_SELECTIVITY;
                }
                num_rows as usize
            }
            Self::Hash { left, right, keys } => {
                let mut num_rows = left.num_rows() as f64 * right.num_rows() as f64;
                for _ in keys {
                    num_rows *= Filter::DEFAULT_SELECTIVITY;
                }
                num_rows as usize
            }
        }
    }

    fn cost(&self) -> f64 {
        match self {
            Self::NestedLoop { left, right, .. } => {
                let join_cost =
                    left.num_rows() as f64 * right.num_rows() as f64 * PlanNode::DEFAULT_ROW_COST;
                left.cost() + right.cost() + join_cost
            }
            Self::Hash { left, right, .. } => {
                let join_cost =
                    (left.num_rows() as f64 + right.num_rows() as f64) * PlanNode::DEFAULT_ROW_COST;
                left.cost() + right.cost() + join_cost
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum CompareOp {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

impl CompareOp {
    pub fn from_binary_op(op: BinaryOp) -> Option<Self> {
        match op {
            BinaryOp::Eq => Some(Self::Eq),
            BinaryOp::Ne => Some(Self::Ne),
            BinaryOp::Lt => Some(Self::Lt),
            BinaryOp::Le => Some(Self::Le),
            BinaryOp::Gt => Some(Self::Gt),
            BinaryOp::Ge => Some(Self::Ge),
            _ => None,
        }
    }

    pub fn to_binary_op(self) -> BinaryOp {
        match self {
            Self::Eq => BinaryOp::Eq,
            Self::Ne => BinaryOp::Ne,
            Self::Lt => BinaryOp::Lt,
            Self::Le => BinaryOp::Le,
            Self::Gt => BinaryOp::Gt,
            Self::Ge => BinaryOp::Ge,
        }
    }

    pub fn flip(self) -> Self {
        match self {
            Self::Eq => Self::Eq,
            Self::Ne => Self::Ne,
            Self::Lt => Self::Gt,
            Self::Le => Self::Ge,
            Self::Gt => Self::Lt,
            Self::Ge => Self::Le,
        }
    }
}

impl std::fmt::Display for CompareOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.to_binary_op().fmt(f)
    }
}

impl<'a> PlanNode<'a> {
    pub fn cross_product(self, other: Self) -> Self {
        match self.simplify(other) {
            Ok(plan) => plan,
            Err((left, right)) => Self::CrossProduct(CrossProduct {
                left: Box::new(left),
                right: Box::new(right),
            }),
        }
    }

    pub fn nested_loop_join(
        self,
        other: Self,
        comparisons: Vec<(CompareOp, PlanExpression<'a>, PlanExpression<'a>)>,
    ) -> Self {
        match self.simplify(other) {
            Ok(plan) => plan,
            Err((left, right)) => Self::Join(Join::NestedLoop {
                left: Box::new(left),
                right: Box::new(right),
                comparisons,
            }),
        }
    }

    pub fn hash_join(
        self,
        other: Self,
        keys: Vec<(PlanExpression<'a>, PlanExpression<'a>)>,
    ) -> Self {
        match self.simplify(other) {
            Ok(plan) => plan,
            Err((left, right)) => Self::Join(Join::Hash {
                left: Box::new(left),
                right: Box::new(right),
                keys,
            }),
        }
    }

    fn simplify(self, other: Self) -> Result<Self, (Self, Self)> {
        if self.produces_no_rows() || other.produces_no_rows() {
            return Ok(PlanNode::new_no_rows(self.outputs()));
        }
        if self.outputs().is_empty() {
            return Ok(other);
        }
        if other.outputs().is_empty() {
            return Ok(self);
        }
        Err((self, other))
    }
}

impl<'a> Planner<'a> {
    pub fn plan_join(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        join: parser::Join,
    ) -> PlannerResult<PlanNode<'a>> {
        let left = self.plan_table_ref(expr_binder, join.left)?;
        let right = self.plan_table_ref(expr_binder, join.right)?;

        let (plan, condition) = match join.condition {
            parser::JoinCondition::On(condition) => {
                let plan = left.cross_product(right);
                expr_binder.bind(plan, condition)?
            }
            parser::JoinCondition::Using(column_names) => {
                let column_map = self.column_map();
                let mut condition = TypedExpression::from(Value::from(true));
                for column_name in column_names {
                    let left_column_ref = left.resolve_column(&column_map, &column_name)?;
                    let right_column_ref = right.resolve_column(&column_map, &column_name)?;
                    condition = condition.and(left_column_ref.eq(right_column_ref)?)?;
                }
                let plan = left.cross_product(right);
                (plan, condition)
            }
        };
        plan.filter(condition)
    }
}

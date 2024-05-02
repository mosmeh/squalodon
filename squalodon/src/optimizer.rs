mod join;

use crate::planner::{
    Aggregate, CrossProduct, Delete, Explain, Filter, Insert, Join, Limit, Plan, PlanNode,
    PlannerResult, Project, Sort, Spool, TopN, Union, Update,
};

pub fn optimize(plan: Plan) -> PlannerResult<Plan> {
    join::optimize(plan)
}

impl PlanNode<'_> {
    /// Recursively find and replace nodes in the plan.
    ///
    /// The `predicate` function is called on each node in the plan.
    /// If the predicate returns true, the node is replaced with the result of
    /// calling `f` on it. Otherwise, the node is left unchanged and the
    /// process continues to its children.
    fn find_and_replace(
        self,
        predicate: impl Fn(&Self) -> bool + Copy,
        f: impl FnOnce(Self) -> PlannerResult<Self> + Copy,
    ) -> PlannerResult<Self> {
        if predicate(&self) {
            return f(self);
        }
        Ok(match self {
            Self::Explain(Explain {
                source,
                output,
                column_map,
            }) => Self::Explain(Explain {
                source: source.find_and_replace(predicate, f)?.into(),
                output,
                column_map,
            }),
            Self::Project(Project {
                source,
                projections,
            }) => Self::Project(Project {
                source: source.find_and_replace(predicate, f)?.into(),
                projections,
            }),
            Self::Filter(Filter { source, conjuncts }) => Self::Filter(Filter {
                source: source.find_and_replace(predicate, f)?.into(),
                conjuncts,
            }),
            Self::Sort(Sort { source, order_by }) => Self::Sort(Sort {
                source: source.find_and_replace(predicate, f)?.into(),
                order_by,
            }),
            Self::Limit(Limit {
                source,
                limit,
                offset,
            }) => Self::Limit(Limit {
                source: source.find_and_replace(predicate, f)?.into(),
                limit,
                offset,
            }),
            Self::TopN(TopN {
                source,
                limit,
                offset,
                order_by,
            }) => Self::TopN(TopN {
                source: source.find_and_replace(predicate, f)?.into(),
                limit,
                offset,
                order_by,
            }),
            Self::CrossProduct(CrossProduct { left, right }) => Self::CrossProduct(CrossProduct {
                left: left.find_and_replace(predicate, f)?.into(),
                right: right.find_and_replace(predicate, f)?.into(),
            }),
            Self::Join(Join::NestedLoop {
                left,
                right,
                comparisons,
            }) => Self::Join(Join::NestedLoop {
                left: left.find_and_replace(predicate, f)?.into(),
                right: right.find_and_replace(predicate, f)?.into(),
                comparisons,
            }),
            Self::Join(Join::Hash { left, right, keys }) => Self::Join(Join::Hash {
                left: left.find_and_replace(predicate, f)?.into(),
                right: right.find_and_replace(predicate, f)?.into(),
                keys,
            }),
            Self::Aggregate(Aggregate::Ungrouped { source, ops }) => {
                Self::Aggregate(Aggregate::Ungrouped {
                    source: source.find_and_replace(predicate, f)?.into(),
                    ops,
                })
            }
            Self::Aggregate(Aggregate::Hash { source, ops }) => Self::Aggregate(Aggregate::Hash {
                source: source.find_and_replace(predicate, f)?.into(),
                ops,
            }),
            Self::Union(Union {
                left,
                right,
                outputs,
            }) => Self::Union(Union {
                left: left.find_and_replace(predicate, f)?.into(),
                right: right.find_and_replace(predicate, f)?.into(),
                outputs,
            }),
            Self::Spool(Spool { source }) => Self::Spool(Spool {
                source: source.find_and_replace(predicate, f)?.into(),
            }),
            Self::Insert(Insert {
                source,
                table,
                output,
            }) => Self::Insert(Insert {
                source: source.find_and_replace(predicate, f)?.into(),
                table,
                output,
            }),
            Self::Update(Update {
                source,
                table,
                output,
            }) => Self::Update(Update {
                source: source.find_and_replace(predicate, f)?.into(),
                table,
                output,
            }),
            Self::Delete(Delete {
                source,
                table,
                output,
            }) => Self::Delete(Delete {
                source: source.find_and_replace(predicate, f)?.into(),
                table,
                output,
            }),
            this => this,
        })
    }
}

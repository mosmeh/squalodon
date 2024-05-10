use crate::planner::{
    ColumnMap, CompareOp, CrossProduct, Filter, Node, Plan, PlanExpression, PlanNode, PlannerResult,
};
use std::{
    cell::RefCell,
    collections::{BTreeSet, HashMap, HashSet},
};

/// Optimizes join order and join algorithms.
pub fn optimize(plan: Plan) -> PlannerResult<Plan> {
    plan.map(|node, column_map| optimize_inner(node, column_map))
}

fn optimize_inner<'a>(
    node: PlanNode<'a>,
    column_map: &RefCell<ColumnMap>,
) -> PlannerResult<PlanNode<'a>> {
    node.find_and_replace(
        |node| match node {
            PlanNode::CrossProduct(_) => true,
            PlanNode::Filter(Filter { source, .. }) => {
                matches!(**source, PlanNode::CrossProduct(_))
            }
            _ => false,
        },
        |node| {
            let original_outputs = node.outputs();

            let mut graph = Graph::new(column_map);
            graph.add(node)?;
            let optimized_plan = graph.into_optimized_plan()?;

            // Join reordering may change the order of columns, so we need to
            // project to the original order.
            let mut column_map = column_map.borrow_mut();
            let exprs = original_outputs
                .into_iter()
                .map(|id| PlanExpression::ColumnRef(id).into_typed(column_map[id].ty()))
                .collect();
            Ok(optimized_plan.project(&mut column_map, exprs))
        },
    )
}

/// Hypergraph representation of join plan.
///
/// This is a graph where each node is a `PlanNode` corresponding to a joined
/// table, and each edge is a join condition.
/// This has to be a hypergraph, not an ordinary graph, because a join condition
/// can reference more than 2 nodes.
struct Graph<'a, 'b> {
    nodes: Vec<PlanNode<'a>>,
    edges: HashSet<Edge<'a>>,
    column_map: &'b RefCell<ColumnMap>,
}

impl<'a, 'b> Graph<'a, 'b> {
    fn new(column_map: &'b RefCell<ColumnMap>) -> Self {
        Self {
            nodes: Vec::new(),
            edges: HashSet::new(),
            column_map,
        }
    }

    fn add(&mut self, node: PlanNode<'a>) -> PlannerResult<BTreeSet<NodeId>> {
        let node = match node {
            PlanNode::CrossProduct(CrossProduct { left, right }) => {
                let mut child_nodes = self.add(*left)?;
                child_nodes.append(&mut self.add(*right)?);
                return Ok(child_nodes);
            }
            PlanNode::Filter(Filter { source, conjuncts }) => {
                if let PlanNode::CrossProduct(CrossProduct { left, right }) = *source {
                    let mut child_nodes = self.add(*left)?;
                    child_nodes.append(&mut self.add(*right)?);
                    for conjunct in conjuncts {
                        let refs = conjunct.referenced_columns();
                        let nodes = if refs.is_empty() {
                            child_nodes.clone()
                        } else {
                            self.nodes
                                .iter()
                                .enumerate()
                                .filter_map(|(i, node)| {
                                    if node.outputs().into_iter().any(|id| refs.contains(&id)) {
                                        Some(NodeId(i))
                                    } else {
                                        None
                                    }
                                })
                                .collect()
                        };
                        self.edges.insert(Edge {
                            nodes,
                            condition: conjunct,
                        });
                    }
                    return Ok(child_nodes);
                }
                PlanNode::Filter(Filter { source, conjuncts })
            }
            PlanNode::Join(_) => unreachable!("Join node should appear only after optimization"),
            node => node,
        };
        let id = NodeId(self.nodes.len());
        self.nodes.push(optimize_inner(node, self.column_map)?);
        Ok(BTreeSet::from([id]))
    }

    fn into_optimized_plan(mut self) -> PlannerResult<PlanNode<'a>> {
        if let Some(plan) = self.generate_optimized_plan()? {
            return Ok(plan);
        }

        // We reach here when the graph is disconnected.
        // We need to add cross products to connect the disconnected components.
        for i in 1..self.nodes.len() {
            for j in 0..i {
                self.edges.insert(Edge {
                    nodes: [NodeId(i), NodeId(j)].into(),
                    condition: PlanExpression::Constant(true.into()),
                });
            }
        }
        self.generate_optimized_plan().map(Option::unwrap)
    }

    fn generate_optimized_plan(&self) -> PlannerResult<Option<PlanNode<'a>>> {
        // An implementation of DPsize algorithm.
        // A pseudo code can be found in:
        // Guido Moerkotte and Thomas Neumann. 2006. Analysis of two existing and one new dynamic programming algorithm for the generation of optimal bushy join trees without cross products. In Proceedings of the 32nd international conference on Very large data bases (VLDB '06). VLDB Endowment, 930–941.
        // https://dl.acm.org/doi/10.5555/1182635.1164207

        let mut best_plans: HashMap<_, _> = self
            .nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (BTreeSet::from([NodeId(i)]), node.clone()))
            .collect();

        for s in 2..=self.nodes.len() {
            let mut next_best_plans = HashMap::new();
            for (s1, p1) in &best_plans {
                // Find all edges that are not internal to s1
                let cross_edges: Vec<_> = self
                    .edges
                    .iter()
                    .filter(|edge| !edge.nodes.is_subset(s1))
                    .collect();
                for (s2, p2) in &best_plans {
                    if s1.len() + s2.len() != s || !s1.is_disjoint(s2) {
                        continue;
                    }
                    let s_union = s1.union(s2).copied().collect();
                    // Find all edges that connect s1 and s2
                    let conjuncts: Vec<_> = cross_edges
                        .iter()
                        .filter(|edge| !edge.nodes.is_subset(s2) && edge.nodes.is_subset(&s_union))
                        .map(|edge| edge.condition.clone())
                        .collect();
                    if conjuncts.is_empty() {
                        // s1 and s2 are disconnected
                        continue;
                    }
                    let plan = p1.clone().best_join(p2.clone(), conjuncts)?;
                    match next_best_plans.entry(s_union) {
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            entry.insert(plan);
                        }
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            // Use the number of produced rows as the cost function
                            if entry.get().num_rows() > plan.num_rows() {
                                entry.insert(plan);
                            }
                        }
                    }
                }
            }
            best_plans.extend(next_best_plans);
        }

        let all_nodes = (0..self.nodes.len()).map(NodeId).collect();
        Ok(best_plans.get(&all_nodes).cloned())
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NodeId(usize);

#[derive(PartialEq, Eq, Hash)]
struct Edge<'a> {
    nodes: BTreeSet<NodeId>,
    condition: PlanExpression<'a>,
}

impl<'a> PlanNode<'a> {
    /// Chooses the best join algorithm, falling back to
    /// `CrossProduct` + `Filter` if no efficient join algorithm is applicable.
    fn best_join(self, other: Self, conjuncts: Vec<PlanExpression<'a>>) -> PlannerResult<Self> {
        let mut conjuncts: HashSet<_> = conjuncts.into_iter().collect();

        let left_outputs = self.outputs().into_iter().collect();
        let right_outputs = other.outputs().into_iter().collect();
        let mut binary_comparisons = Vec::new();
        conjuncts.retain(|conjunct| {
            let PlanExpression::BinaryOp { op, lhs, rhs } = conjunct else {
                return true;
            };
            let Some(op) = CompareOp::from_binary_op(*op) else {
                return true;
            };
            let lhs_refs = lhs.referenced_columns();
            if lhs_refs.is_empty() {
                return true;
            }
            let rhs_refs = rhs.referenced_columns();
            if rhs_refs.is_empty() {
                return true;
            }
            let (op, left, right) =
                if lhs_refs.is_subset(&left_outputs) && rhs_refs.is_subset(&right_outputs) {
                    (op, lhs, rhs)
                } else if lhs_refs.is_subset(&right_outputs) && rhs_refs.is_subset(&left_outputs) {
                    (op.flip(), rhs, lhs)
                } else {
                    return true;
                };
            binary_comparisons.push((op, (**left).clone(), (**right).clone()));
            false
        });

        let has_inequality = binary_comparisons
            .iter()
            .any(|(op, _, _)| *op != CompareOp::Eq);
        let inner = if binary_comparisons.is_empty() {
            self.cross_product(other)
        } else if has_inequality {
            // TODO: compare costs of NestedLoopJoin and HashJoin + Filter when
            //       some of the conjuncts are equalities and others are
            //       inequalities
            self.nested_loop_join(other, binary_comparisons)
        } else {
            let keys = binary_comparisons
                .into_iter()
                .map(|(_, left, right)| (left, right))
                .collect();
            self.hash_join(other, keys)
        };
        inner.filter_with_conjuncts(conjuncts)
    }
}

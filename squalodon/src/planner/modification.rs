use super::{
    expression::{ExpressionBinder, TypedExpression},
    Explain, ExplainVisitor, Plan, PlanNode, Planner, PlannerError, PlannerResult,
};
use crate::{parser, planner, rows::ColumnIndex, storage::Table, Storage};

pub struct Insert<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
}

impl<T: Storage> Explain for Insert<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Insert table={:?}", self.table.name());
        self.source.visit(visitor);
    }
}

pub struct Update<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
}

impl<T: Storage> Explain for Update<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Update table={:?}", self.table.name());
        self.source.visit(visitor);
    }
}

pub struct Delete<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
}

impl<T: Storage> Explain for Delete<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Delete table={:?}", self.table.name());
        self.source.visit(visitor);
    }
}

impl<'txn, 'db, T: Storage> Planner<'txn, 'db, T> {
    pub fn plan_insert(&self, insert: parser::Insert) -> PlannerResult<Plan<'txn, 'db, T>> {
        let table = self.catalog.table(insert.table_name)?;
        let Plan { node, schema } = self.plan_query(insert.query)?;
        if table.columns().len() != schema.0.len() {
            return Err(PlannerError::ColumnCountMismatch {
                expected: table.columns().len(),
                actual: schema.0.len(),
            });
        }
        let column_mapping: Vec<_> = if let Some(column_names) = insert.column_names {
            if table.columns().len() != column_names.len() {
                return Err(PlannerError::ColumnCountMismatch {
                    expected: table.columns().len(),
                    actual: column_names.len(),
                });
            }
            let mut mapping = vec![None; table.columns().len()];
            let iter = column_names.into_iter().zip(schema.0).enumerate();
            for (source_index, (column_name, source_column)) in iter {
                let (dest_index, dest_column) = table
                    .columns()
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name == column_name)
                    .ok_or_else(|| PlannerError::UnknownColumn(column_name.clone()))?;
                match &mut mapping[dest_index] {
                    Some(_) => return Err(PlannerError::DuplicateColumn(column_name)),
                    i @ None => *i = Some((source_index, source_column, dest_column)),
                }
            }
            mapping.into_iter().map(Option::unwrap).collect()
        } else {
            schema
                .0
                .into_iter()
                .zip(table.columns())
                .enumerate()
                .map(|(i, (source_column, dest_column))| (i, source_column, dest_column))
                .collect()
        };
        let mut exprs = Vec::with_capacity(table.columns().len());
        for (source_index, source_column, dest_column) in column_mapping {
            let mut expr = planner::Expression::ColumnRef(ColumnIndex(source_index));
            if !source_column.ty.is_compatible_with(dest_column.ty) {
                if !source_column.ty.can_cast_to(dest_column.ty) {
                    return Err(PlannerError::TypeError);
                }
                expr = planner::Expression::Cast {
                    expr: Box::new(expr),
                    ty: dest_column.ty,
                };
            }
            exprs.push(expr);
        }
        let node = PlanNode::Project(planner::Project {
            source: Box::new(node),
            exprs,
        });

        // Spool prevents Halloween problem.
        let node = PlanNode::Spool(planner::Spool {
            source: Box::new(node),
        });

        Ok(Plan {
            node: PlanNode::Insert(Insert {
                source: Box::new(node),
                table,
            }),
            schema: vec![planner::Column::new("count", planner::Type::Integer)].into(),
        })
    }

    pub fn plan_update(&self, update: parser::Update) -> PlannerResult<Plan<'txn, 'db, T>> {
        let table = self.catalog.table(update.table_name)?;
        let expr_binder = ExpressionBinder::new(self);

        let mut plan = self.plan_base_table(table.clone());
        if let Some(where_clause) = update.where_clause {
            plan = self.plan_where_clause(&expr_binder, plan, where_clause)?;
        }

        let mut exprs = vec![None; table.columns().len()];
        for set in update.sets {
            let (dest_index, column) = table
                .columns()
                .iter()
                .enumerate()
                .find(|(_, column)| column.name == set.column_name)
                .ok_or_else(|| PlannerError::UnknownColumn(set.column_name))?;
            let expr = &mut exprs[dest_index];
            if expr.is_some() {
                return Err(PlannerError::DuplicateColumn(column.name.clone()));
            }
            let (
                new_plan,
                TypedExpression {
                    expr: mut bound_expr,
                    ty,
                },
            ) = expr_binder.bind(plan, set.expr)?;
            plan = new_plan;
            if !ty.is_compatible_with(column.ty) {
                if !ty.can_cast_to(column.ty) {
                    return Err(PlannerError::TypeError);
                }
                bound_expr = planner::Expression::Cast {
                    expr: Box::new(bound_expr),
                    ty: column.ty,
                };
            }
            *expr = Some(bound_expr);
        }

        // The input of the Update node should consist of the old and new values
        // of columns concatenated together.
        let exprs =
            (0..table.columns().len())
                .map(|i| planner::Expression::ColumnRef(ColumnIndex(i)))
                .chain(exprs.into_iter().enumerate().map(|(i, expr)| {
                    expr.unwrap_or(planner::Expression::ColumnRef(ColumnIndex(i)))
                }))
                .collect();
        let node = PlanNode::Project(planner::Project {
            source: Box::new(plan.node),
            exprs,
        });

        // Spool prevents Halloween problem.
        let node = PlanNode::Spool(planner::Spool {
            source: Box::new(node),
        });

        Ok(Plan {
            node: PlanNode::Update(Update {
                source: Box::new(node),
                table,
            }),
            schema: vec![planner::Column::new("count", planner::Type::Integer)].into(),
        })
    }

    pub fn plan_delete(&self, delete: parser::Delete) -> PlannerResult<Plan<'txn, 'db, T>> {
        let table = self.catalog.table(delete.table_name)?;
        let mut plan = self.plan_base_table(table.clone());
        if let Some(where_clause) = delete.where_clause {
            plan = self.plan_where_clause(&ExpressionBinder::new(self), plan, where_clause)?;
        }

        // Spool prevents Halloween problem.
        let node = PlanNode::Spool(planner::Spool {
            source: Box::new(plan.node),
        });

        Ok(Plan {
            node: PlanNode::Delete(Delete {
                source: Box::new(node),
                table,
            }),
            schema: vec![planner::Column::new("count", planner::Type::Integer)].into(),
        })
    }
}

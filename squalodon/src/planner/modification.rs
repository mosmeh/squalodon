use super::{Binder, Explain, ExplainVisitor, Plan, PlanNode, PlannerError, PlannerResult};
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

impl<'txn, 'db, T: Storage> Binder<'txn, 'db, T> {
    pub fn bind_insert(&self, insert: parser::Insert) -> PlannerResult<Plan<'txn, 'db, T>> {
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
        Ok(Plan::sink(PlanNode::Insert(Insert {
            source: Box::new(node),
            table,
        })))
    }

    pub fn bind_update(&self, update: parser::Update) -> PlannerResult<Plan<'txn, 'db, T>> {
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
        Ok(Plan::sink(PlanNode::Update(Update {
            source: Box::new(node),
            table,
        })))
    }

    pub fn bind_delete(&self, delete: parser::Delete) -> PlannerResult<Plan<'txn, 'db, T>> {
        let table = self.catalog.table(delete.table_name)?;
        let mut plan = self.bind_base_table(table.clone());
        if let Some(where_clause) = delete.where_clause {
            plan = self.bind_where_clause(plan, where_clause)?;
        }
        Ok(Plan::sink(PlanNode::Delete(Delete {
            source: Box::new(plan.node),
            table,
        })))
    }
}

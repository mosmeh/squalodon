use super::{
    expression::{ExpressionBinder, TypedExpression},
    Column, ColumnId, ColumnMap, ExplainFormatter, Node, PlanNode, Planner, PlannerError,
    PlannerResult,
};
use crate::{parser, planner, storage::Table, Storage, Type};

pub struct Insert<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
    output: ColumnId,
}

impl<T: Storage> Node for Insert<'_, '_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        write!(f, "Insert on {}", self.table.name());
        self.source.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.push(self.output);
    }
}

pub struct Update<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
    output: ColumnId,
}

impl<T: Storage> Node for Update<'_, '_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        write!(f, "Update on {}", self.table.name());
        self.source.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.push(self.output);
    }
}

pub struct Delete<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
    output: ColumnId,
}

impl<T: Storage> Node for Delete<'_, '_, T> {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        write!(f, "Delete on {}", self.table.name());
        self.source.fmt_explain(f);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.push(self.output);
    }
}

impl<'txn, 'db, T: Storage> PlanNode<'txn, 'db, T> {
    fn insert(self, column_map: &mut ColumnMap, table: Table<'txn, 'db, T>) -> Self {
        Self::Insert(Insert {
            source: Box::new(self),
            table,
            output: column_map.insert(Column::new("count", Type::Integer)),
        })
    }

    fn update(self, column_map: &mut ColumnMap, table: Table<'txn, 'db, T>) -> Self {
        Self::Update(Update {
            source: Box::new(self),
            table,
            output: column_map.insert(Column::new("count", Type::Integer)),
        })
    }

    fn delete(self, column_map: &mut ColumnMap, table: Table<'txn, 'db, T>) -> Self {
        Self::Delete(Delete {
            source: Box::new(self),
            table,
            output: column_map.insert(Column::new("count", Type::Integer)),
        })
    }
}

impl<'txn, 'db, T: Storage> Planner<'txn, 'db, T> {
    pub fn plan_insert(&self, insert: parser::Insert) -> PlannerResult<PlanNode<'txn, 'db, T>> {
        let table = self.ctx.catalog().table(insert.table_name)?;
        let plan = self.plan_query(insert.query)?;
        let outputs = plan.outputs();
        if table.columns().len() != outputs.len() {
            return Err(PlannerError::ColumnCountMismatch {
                expected: table.columns().len(),
                actual: outputs.len(),
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
            let iter = column_names.into_iter().zip(outputs);
            for (column_name, source_column_id) in iter {
                let (dest_index, dest_column) = table
                    .columns()
                    .iter()
                    .enumerate()
                    .find(|(_, column)| column.name == column_name)
                    .ok_or_else(|| PlannerError::UnknownColumn(column_name.clone()))?;
                match &mut mapping[dest_index] {
                    Some(_) => return Err(PlannerError::DuplicateColumn(column_name)),
                    i @ None => *i = Some((source_column_id, dest_column)),
                }
            }
            mapping.into_iter().map(Option::unwrap).collect()
        } else {
            outputs.into_iter().zip(table.columns()).collect()
        };
        let mut exprs = Vec::with_capacity(table.columns().len());
        let mut column_map = self.column_map();
        for (source_column_id, dest_column) in column_mapping {
            let mut expr = planner::Expression::ColumnRef(source_column_id);
            let source_column = &column_map[source_column_id];
            if !source_column.ty.is_compatible_with(dest_column.ty) {
                if !source_column.ty.can_cast_to(dest_column.ty) {
                    return Err(PlannerError::TypeError);
                }
                expr = expr.cast(dest_column.ty);
            }
            exprs.push(TypedExpression {
                expr,
                ty: dest_column.ty.into(),
            });
        }
        let plan = plan.project(&mut column_map, exprs);

        // Spool prevents Halloween problem.
        let plan = plan.spool();

        Ok(plan.insert(&mut column_map, table))
    }

    pub fn plan_update(&self, update: parser::Update) -> PlannerResult<PlanNode<'txn, 'db, T>> {
        let table = self.ctx.catalog().table(update.table_name)?;
        let expr_binder = ExpressionBinder::new(self);

        let mut plan = self.plan_base_table(table.clone());
        if let Some(where_clause) = update.where_clause {
            plan = self.plan_filter(&expr_binder, plan, where_clause)?;
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
                bound_expr = bound_expr.cast(column.ty);
            }
            *expr = Some(TypedExpression {
                expr: bound_expr,
                ty: column.ty.into(),
            });
        }

        // The input of the Update node should consist of the old and new values
        // of columns concatenated together.
        let outputs = plan.outputs();
        let mut column_map = self.column_map();
        let old = outputs.into_iter().map(|id| TypedExpression {
            expr: planner::Expression::ColumnRef(id),
            ty: column_map[id].ty,
        });
        let new = exprs
            .into_iter()
            .zip(old.clone())
            .map(|(expr, old)| expr.unwrap_or(old));
        let exprs = old.chain(new).collect();
        let plan = plan.project(&mut column_map, exprs);

        // Spool prevents Halloween problem.
        let plan = plan.spool();

        Ok(plan.update(&mut column_map, table))
    }

    pub fn plan_delete(&self, delete: parser::Delete) -> PlannerResult<PlanNode<'txn, 'db, T>> {
        let table = self.ctx.catalog().table(delete.table_name)?;
        let mut plan = self.plan_base_table(table.clone());
        if let Some(where_clause) = delete.where_clause {
            plan = self.plan_filter(&ExpressionBinder::new(self), plan, where_clause)?;
        }

        // Spool prevents Halloween problem.
        let plan = plan.spool();

        Ok(plan.delete(&mut self.column_map(), table))
    }
}

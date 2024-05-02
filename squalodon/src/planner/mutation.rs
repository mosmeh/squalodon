use super::{
    expression::{ExpressionBinder, PlanExpression, TypedExpression},
    Column, ColumnId, ColumnMap, ExplainFormatter, Node, PlanNode, Planner, PlannerError,
    PlannerResult,
};
use crate::{catalog::Table, parser, Type};

#[derive(Clone)]
pub struct Insert<'a> {
    pub source: Box<PlanNode<'a>>,
    pub table: Table<'a>,
    pub output: ColumnId,
}

impl Node for Insert<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("Insert")
            .field("table", self.table.name())
            .child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.push(self.output);
    }

    fn num_rows(&self) -> usize {
        1
    }

    fn cost(&self) -> f64 {
        self.source.num_rows() as f64 * PlanNode::DEFAULT_ROW_COST
    }
}

#[derive(Clone)]
pub struct Update<'a> {
    pub source: Box<PlanNode<'a>>,
    pub table: Table<'a>,
    pub output: ColumnId,
}

impl Node for Update<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("Update")
            .field("table", self.table.name())
            .child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.push(self.output);
    }

    fn num_rows(&self) -> usize {
        1
    }

    fn cost(&self) -> f64 {
        self.source.num_rows() as f64 * PlanNode::DEFAULT_ROW_COST
    }
}

#[derive(Clone)]
pub struct Delete<'a> {
    pub source: Box<PlanNode<'a>>,
    pub table: Table<'a>,
    pub output: ColumnId,
}

impl Node for Delete<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("Delete")
            .field("table", self.table.name())
            .child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.push(self.output);
    }

    fn num_rows(&self) -> usize {
        1
    }

    fn cost(&self) -> f64 {
        self.source.num_rows() as f64 * PlanNode::DEFAULT_ROW_COST
    }
}

impl<'a> PlanNode<'a> {
    fn insert(self, column_map: &mut ColumnMap, table: Table<'a>) -> Self {
        Self::Insert(Insert {
            source: Box::new(self),
            table,
            output: column_map.insert(Column::new("count", Type::Integer)),
        })
    }

    fn update(self, column_map: &mut ColumnMap, table: Table<'a>) -> Self {
        Self::Update(Update {
            source: Box::new(self),
            table,
            output: column_map.insert(Column::new("count", Type::Integer)),
        })
    }

    fn delete(self, column_map: &mut ColumnMap, table: Table<'a>) -> Self {
        Self::Delete(Delete {
            source: Box::new(self),
            table,
            output: column_map.insert(Column::new("count", Type::Integer)),
        })
    }
}

impl<'a> Planner<'a> {
    pub fn plan_insert(&self, insert: parser::Insert) -> PlannerResult<PlanNode<'a>> {
        let table = self.catalog.table(&insert.table_name)?;
        let mut plan = self.plan_query(insert.query)?;
        let outputs = plan.outputs();

        if outputs.len() > table.columns().len() {
            return Err(PlannerError::ColumnCountMismatch {
                expected: table.columns().len(),
                actual: outputs.len(),
            });
        }

        let column_mapping = if let Some(column_names) = insert.column_names {
            let mut mapping = vec![None; table.columns().len()];
            for (column_name, source_column_id) in column_names.into_iter().zip(outputs) {
                let dest_index = table
                    .columns()
                    .iter()
                    .position(|column| column.name == column_name)
                    .ok_or_else(|| PlannerError::UnknownColumn(column_name.clone()))?;
                match &mut mapping[dest_index] {
                    Some(_) => return Err(PlannerError::DuplicateColumn(column_name)),
                    i @ None => *i = Some(source_column_id),
                }
            }
            mapping
        } else {
            outputs
                .into_iter()
                .map(Some)
                .chain(std::iter::repeat(None))
                .take(table.columns().len())
                .collect()
        };

        let num_provided_columns = column_mapping
            .iter()
            .zip(table.columns())
            .filter(|(mapping, column)| mapping.is_some() || column.default_value.is_some())
            .count();
        if num_provided_columns != table.columns().len() {
            return Err(PlannerError::ColumnCountMismatch {
                expected: table.columns().len(),
                actual: num_provided_columns,
            });
        }

        let mut exprs = Vec::with_capacity(table.columns().len());
        let mut column_map = self.column_map_mut();
        for (source_column_id, dest_column) in column_mapping.into_iter().zip(table.columns()) {
            let TypedExpression { mut expr, ty } = if let Some(id) = source_column_id {
                PlanExpression::ColumnRef(id).into_typed(column_map[id].ty)
            } else {
                let default_value = dest_column.default_value.clone().unwrap();
                let (new_plan, expr) = ExpressionBinder::new(self).bind(plan, default_value)?;
                plan = new_plan;
                expr
            };
            if !ty.is_compatible_with(dest_column.ty) {
                if !ty.can_cast_to(dest_column.ty) {
                    return Err(PlannerError::TypeError);
                }
                expr = expr.cast(dest_column.ty);
            }
            exprs.push(expr.into_typed(dest_column.ty));
        }
        let plan = plan.project(&mut column_map, exprs);

        // Spool prevents Halloween problem.
        let plan = plan.spool();

        Ok(plan.insert(&mut column_map, table))
    }

    pub fn plan_update(&self, update: parser::Update) -> PlannerResult<PlanNode<'a>> {
        let table = self.catalog.table(&update.table_name)?;
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
            *expr = Some(bound_expr.into_typed(column.ty));
        }

        // The input of the Update node should consist of the old and new values
        // of columns concatenated together.
        let outputs = plan.outputs();
        let mut column_map = self.column_map_mut();
        let old = outputs
            .into_iter()
            .map(|id| PlanExpression::ColumnRef(id).into_typed(column_map[id].ty));
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

    pub fn plan_delete(&self, delete: parser::Delete) -> PlannerResult<PlanNode<'a>> {
        let table = self.catalog.table(&delete.table_name)?;
        let mut plan = self.plan_base_table(table.clone());
        if let Some(where_clause) = delete.where_clause {
            plan = self.plan_filter(&ExpressionBinder::new(self), plan, where_clause)?;
        }

        // Spool prevents Halloween problem.
        let plan = plan.spool();

        Ok(plan.delete(&mut self.column_map_mut(), table))
    }
}

use super::{ColumnId, ExplainFormatter, Node, PlanNode, Planner, PlannerError, PlannerResult};
use crate::{catalog, parser, rows::ColumnIndex, storage::Transaction};
use std::collections::HashSet;

pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<catalog::Column>,
    pub constraints: Vec<catalog::Constraint>,
    pub create_indexes: Vec<CreateIndex>,
}

impl Node for CreateTable {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.write_str("CreateTable");
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}
}

pub struct CreateIndex {
    pub name: String,
    pub table_name: String,
    pub column_indexes: Vec<ColumnIndex>,
    pub is_unique: bool,
}

impl Node for CreateIndex {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.write_str("CreateIndex");
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}
}

pub struct DropObject(pub parser::DropObject);

impl Node for DropObject {
    fn fmt_explain(&self, f: &mut ExplainFormatter) {
        f.write_str("Drop");
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}
}

impl<T> PlanNode<'_, T> {
    fn new_create_table(create_table: CreateTable) -> Self {
        Self::CreateTable(create_table)
    }

    fn new_create_index(create_index: CreateIndex) -> Self {
        Self::CreateIndex(create_index)
    }

    fn new_drop(drop_object: parser::DropObject) -> Self {
        Self::Drop(DropObject(drop_object))
    }
}

impl<'a, T> Planner<'a, T> {
    #[allow(clippy::unused_self)]
    pub fn plan_create_table(
        &self,
        create_table: parser::CreateTable,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let mut column_names = HashSet::new();
        for column in &create_table.columns {
            if !column_names.insert(column.name.as_str()) {
                return Err(PlannerError::DuplicateColumn(column.name.clone()));
            }
        }
        let mut bound_constraints = HashSet::new();
        let mut has_primary_key = false;
        let mut create_indexes = Vec::new();
        for constraint in create_table.constraints {
            match constraint {
                parser::Constraint::PrimaryKey(column_names) => {
                    if has_primary_key {
                        return Err(PlannerError::MultiplePrimaryKeys);
                    }
                    has_primary_key = true;
                    let column_indexes =
                        column_indexes_from_names(&create_table.columns, &column_names)?;
                    for column_index in &column_indexes {
                        bound_constraints.insert(catalog::Constraint::NotNull(*column_index));
                    }
                    bound_constraints.insert(catalog::Constraint::PrimaryKey(column_indexes));
                }
                parser::Constraint::NotNull(column_name) => {
                    let index = create_table
                        .columns
                        .iter()
                        .position(|def| def.name == column_name)
                        .ok_or_else(|| PlannerError::UnknownColumn(column_name.clone()))?;
                    bound_constraints.insert(catalog::Constraint::NotNull(ColumnIndex(index)));
                }
                parser::Constraint::Unique(column_names) => {
                    let column_indexes =
                        column_indexes_from_names(&create_table.columns, &column_names)?;
                    let mut name = create_table.name.clone();
                    name.push('_');
                    for column_name in column_names {
                        name.push_str(&column_name);
                        name.push('_');
                    }
                    name.push_str("key");
                    create_indexes.push(CreateIndex {
                        name,
                        table_name: create_table.name.clone(),
                        column_indexes,
                        is_unique: true,
                    });
                }
            }
        }
        if !has_primary_key {
            return Err(PlannerError::NoPrimaryKey);
        }
        let create_table = CreateTable {
            name: create_table.name,
            if_not_exists: create_table.if_not_exists,
            columns: create_table.columns,
            constraints: bound_constraints.into_iter().collect(),
            create_indexes,
        };
        Ok(PlanNode::new_create_table(create_table))
    }

    #[allow(clippy::unused_self)]
    pub fn plan_drop(&self, drop_object: parser::DropObject) -> PlanNode<'a, T> {
        PlanNode::new_drop(drop_object)
    }
}

impl<'a, T: Transaction> Planner<'a, T> {
    pub fn plan_create_index(
        &self,
        create_index: parser::CreateIndex,
    ) -> PlannerResult<PlanNode<'a, T>> {
        let table = self.ctx.catalog().table(create_index.table_name)?;
        let column_indexes =
            column_indexes_from_names(table.columns(), &create_index.column_names)?;
        let create_index = CreateIndex {
            name: create_index.name,
            table_name: table.name().to_owned(),
            column_indexes,
            is_unique: create_index.is_unique,
        };
        Ok(PlanNode::new_create_index(create_index))
    }
}

fn column_indexes_from_names(
    columns: &[catalog::Column],
    column_names: &[String],
) -> PlannerResult<Vec<ColumnIndex>> {
    let mut dedup_set = HashSet::new();
    let mut column_indices = Vec::with_capacity(column_names.len());
    for column_name in column_names {
        if !dedup_set.insert(column_name) {
            return Err(PlannerError::DuplicateColumn(column_name.clone()));
        }
        let index = columns
            .iter()
            .position(|def| def.name == *column_name)
            .ok_or_else(|| PlannerError::UnknownColumn(column_name.clone()))?;
        column_indices.push(ColumnIndex(index));
    }
    Ok(column_indices)
}

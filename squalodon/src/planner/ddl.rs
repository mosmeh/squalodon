use super::{Explain, ExplainVisitor, Plan, PlanNode, Planner, PlannerError, PlannerResult};
use crate::{catalog, parser, rows::ColumnIndex, Storage};
use std::{collections::HashSet, fmt::Write};

pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<catalog::Column>,
    pub constraints: Vec<catalog::Constraint>,
    pub create_indexes: Vec<CreateIndex>,
}

impl Explain for CreateTable {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut s = format!("CreateTable name={:?} columns=[", self.name);
        for (i, column) in self.columns.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            write!(&mut s, "{:?} {:?}", column.name, column.ty).unwrap();
        }
        s.push(']');
        visitor.write_str(&s);
    }
}

pub struct CreateIndex {
    pub name: String,
    pub table_name: String,
    pub column_indexes: Vec<ColumnIndex>,
    pub is_unique: bool,
}

impl Explain for CreateIndex {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut s = format!(
            "CreateIndex name={:?} table={:?} column_indexes=[",
            self.name, self.table_name,
        );
        for (i, index) in self.column_indexes.iter().enumerate() {
            if i > 0 {
                s.push_str(", ");
            }
            write!(&mut s, "{index}").unwrap();
        }
        write!(&mut s, "] is_unique={}", self.is_unique).unwrap();
        visitor.write_str(&s);
    }
}

pub struct DropObject(pub parser::DropObject);

impl Explain for DropObject {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Drop {:?} {:?}", self.0.kind, self.0.name);
    }
}

impl<'txn, 'db, T: Storage> Planner<'txn, 'db, T> {
    #[allow(clippy::unused_self)]
    pub fn plan_create_table(
        &self,
        create_table: parser::CreateTable,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
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
        Ok(Plan::sink(PlanNode::CreateTable(create_table)))
    }

    pub fn plan_create_index(
        &self,
        create_index: parser::CreateIndex,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        let table = self.catalog.table(create_index.table_name)?;
        let column_indexes =
            column_indexes_from_names(table.columns(), &create_index.column_names)?;
        let create_index = CreateIndex {
            name: create_index.name,
            table_name: table.name().to_owned(),
            column_indexes,
            is_unique: create_index.is_unique,
        };
        Ok(Plan::sink(PlanNode::CreateIndex(create_index)))
    }

    #[allow(clippy::unused_self)]
    pub fn plan_drop(&self, drop_object: parser::DropObject) -> Plan<'txn, 'db, T> {
        Plan::sink(PlanNode::Drop(DropObject(drop_object)))
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

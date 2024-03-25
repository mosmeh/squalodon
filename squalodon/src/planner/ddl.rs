use super::{Explain, ExplainVisitor, Plan, PlanNode, Planner, PlannerError, PlannerResult};
use crate::{catalog, parser, planner, rows::ColumnIndex, CatalogError, Storage};
use std::collections::HashSet;

pub struct CreateTable {
    pub name: String,
    pub columns: Vec<catalog::Column>,
    pub constraints: Vec<catalog::Constraint>,
}

impl Explain for CreateTable {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "CreateTable {:?}", self.name);
    }
}

pub struct DropTable {
    pub name: String,
}

impl Explain for DropTable {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "DropTable {:?}", self.name);
    }
}

impl<'txn, 'db, T: Storage> Planner<'txn, 'db, T> {
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
        match self.catalog.table(create_table.name.clone()) {
            Ok(_) if create_table.if_not_exists => return Ok(Plan::empty_source()),
            Ok(_) => return Err(PlannerError::TableAlreadyExists(create_table.name)),
            Err(CatalogError::UnknownEntry(_, _)) => (),
            Err(err) => return Err(err.into()),
        };
        let mut bound_constraints = HashSet::new();
        let mut has_primary_key = false;
        for constraint in create_table.constraints {
            match constraint {
                parser::Constraint::PrimaryKey(columns) => {
                    if has_primary_key {
                        return Err(PlannerError::MultiplePrimaryKeys);
                    }
                    has_primary_key = true;
                    let mut indices = Vec::with_capacity(columns.len());
                    let mut column_names = HashSet::new();
                    for column in &columns {
                        if !column_names.insert(column.as_str()) {
                            return Err(PlannerError::DuplicateColumn(column.clone()));
                        }
                        let (index, _) = create_table
                            .columns
                            .iter()
                            .enumerate()
                            .find(|(_, def)| def.name == *column)
                            .ok_or_else(|| PlannerError::UnknownColumn(column.clone()))?;
                        let index = ColumnIndex(index);
                        indices.push(index);
                        bound_constraints.insert(catalog::Constraint::NotNull(index));
                    }
                    bound_constraints.insert(catalog::Constraint::PrimaryKey(indices));
                }
                parser::Constraint::NotNull(column) => {
                    let (index, _) = create_table
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_, def)| def.name == column)
                        .ok_or_else(|| PlannerError::UnknownColumn(column.clone()))?;
                    bound_constraints.insert(catalog::Constraint::NotNull(ColumnIndex(index)));
                }
            }
        }
        if !has_primary_key {
            return Err(PlannerError::NoPrimaryKey);
        }
        let create_table = planner::CreateTable {
            name: create_table.name,
            columns: create_table.columns,
            constraints: bound_constraints.into_iter().collect(),
        };
        Ok(Plan::sink(PlanNode::CreateTable(create_table)))
    }

    pub fn plan_drop_table(
        &self,
        drop_table: parser::DropTable,
    ) -> PlannerResult<Plan<'txn, 'db, T>> {
        match self.catalog.table(drop_table.name.clone()) {
            Ok(_) => Ok(Plan::sink(PlanNode::DropTable(planner::DropTable {
                name: drop_table.name,
            }))),
            Err(CatalogError::UnknownEntry(_, _)) if drop_table.if_exists => {
                Ok(Plan::empty_source())
            }
            Err(err) => Err(err.into()),
        }
    }
}

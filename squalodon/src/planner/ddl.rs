use super::{ColumnId, ExplainFormatter, Node, PlanNode, Planner, PlannerError, PlannerResult};
use crate::{
    catalog::{self, Index, Table},
    parser,
    rows::ColumnIndex,
};
use std::collections::HashSet;

pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<catalog::Column>,
    pub primary_keys: Vec<ColumnIndex>,
    pub constraints: Vec<catalog::Constraint>,
    pub create_indexes: Vec<CreateIndex>,
}

impl Node for CreateTable {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("CreateTable");
        node.field("name", &self.name);
        for column in &self.columns {
            node.field("column", &column.name);
        }
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
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("CreateIndex");
        node.field("name", &self.name)
            .field("table", &self.table_name);
        for column_index in &self.column_indexes {
            node.field("column", column_index);
        }
        node.field("unique", self.is_unique);
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}
}

pub struct DropObject(pub parser::DropObject);

impl Node for DropObject {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("Drop")
            .field("kind", &self.0.kind)
            .field("name", &self.0.name);
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}
}

pub enum Reindex<'a> {
    Table(Table<'a>),
    Index(Index<'a>),
}

impl Node for Reindex<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Reindex");
        match self {
            Self::Table(table) => node.field("table", table.name()),
            Self::Index(index) => node.field("index", index.name()),
        };
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}
}

impl<'a> PlanNode<'a> {
    fn new_create_table(create_table: CreateTable) -> Self {
        Self::CreateTable(create_table)
    }

    fn new_create_index(create_index: CreateIndex) -> Self {
        Self::CreateIndex(create_index)
    }

    fn new_drop(drop_object: parser::DropObject) -> Self {
        Self::Drop(DropObject(drop_object))
    }

    fn new_reindex(reindex: Reindex<'a>) -> Self {
        Self::Reindex(reindex)
    }
}

impl<'a> Planner<'a> {
    #[allow(clippy::unused_self)]
    pub fn plan_create_table(
        &self,
        create_table: parser::CreateTable,
    ) -> PlannerResult<PlanNode<'a>> {
        let mut column_names = HashSet::new();
        for column in &create_table.columns {
            if !column_names.insert(column.name.as_str()) {
                return Err(PlannerError::DuplicateColumn(column.name.clone()));
            }
        }
        let mut primary_keys = None;
        let mut bound_constraints = HashSet::new();
        let mut create_indexes = Vec::new();
        for constraint in create_table.constraints {
            match constraint {
                parser::Constraint::PrimaryKey(column_names) => {
                    if primary_keys.is_some() {
                        return Err(PlannerError::MultiplePrimaryKeys);
                    }
                    let column_indexes =
                        column_indexes_from_names(&create_table.columns, &column_names)?;
                    for column_index in &column_indexes {
                        bound_constraints.insert(catalog::Constraint::NotNull(*column_index));
                    }
                    primary_keys = Some(column_indexes.clone());
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
        if primary_keys.is_none() {
            return Err(PlannerError::NoPrimaryKey);
        }
        let create_table = CreateTable {
            name: create_table.name,
            if_not_exists: create_table.if_not_exists,
            columns: create_table.columns,
            primary_keys: primary_keys.unwrap(),
            constraints: bound_constraints.into_iter().collect(),
            create_indexes,
        };
        Ok(PlanNode::new_create_table(create_table))
    }

    pub fn plan_create_index(
        &self,
        create_index: parser::CreateIndex,
    ) -> PlannerResult<PlanNode<'a>> {
        let table = self.ctx.catalog().table(&create_index.table_name)?;
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

    #[allow(clippy::unused_self)]
    pub fn plan_drop(&self, drop_object: parser::DropObject) -> PlanNode<'a> {
        PlanNode::new_drop(drop_object)
    }

    pub fn plan_reindex(&self, reindex: parser::Reindex) -> PlannerResult<PlanNode<'a>> {
        let catalog = self.ctx.catalog();
        let reindex = match reindex.kind {
            parser::ObjectKind::Table => {
                let table = catalog.table(&reindex.name)?;
                Reindex::Table(table)
            }
            parser::ObjectKind::Index => {
                let index = catalog.index(&reindex.name)?;
                Reindex::Index(index)
            }
        };
        Ok(PlanNode::new_reindex(reindex))
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

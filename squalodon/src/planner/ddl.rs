use super::{ColumnId, ExplainFormatter, Node, PlanNode, Planner, PlannerError, PlannerResult};
use crate::{
    catalog::{self, CatalogResult, Index, Table},
    parser,
    rows::ColumnIndex,
    CatalogError,
};
use std::collections::HashSet;

pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<catalog::Column>,
    pub primary_keys: Vec<ColumnIndex>,
    pub constraints: Vec<Constraint>,
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

#[derive(PartialEq, Eq, Hash)]
pub enum Constraint {
    Unique(Vec<ColumnIndex>),
}

pub struct CreateIndex<'a> {
    pub name: String,
    pub table: Table<'a>,
    pub column_indexes: Vec<ColumnIndex>,
    pub is_unique: bool,
}

impl Node for CreateIndex<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("CreateIndex");
        node.field("name", &self.name)
            .field("table", self.table.name());
        for column_index in &self.column_indexes {
            node.field("column", &self.table.columns()[column_index.0].name);
        }
        node.field("unique", self.is_unique);
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}
}

pub enum DropObject<'a> {
    Table(Table<'a>),
    Index { table: Table<'a>, name: String },
}

impl Node for DropObject<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let name = match self {
            Self::Table(table) => table.name(),
            Self::Index { name, .. } => name,
        };
        f.node("Drop").field("name", name);
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}
}

pub struct Truncate<'a> {
    pub table: Table<'a>,
}

impl Node for Truncate<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("Truncate").field("table", self.table.name());
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}
}

pub enum Analyze<'a> {
    AllTables,
    Tables(Vec<Table<'a>>),
}

impl Node for Analyze<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Analyze");
        match self {
            Self::AllTables => {
                node.field("all tables", true);
            }
            Self::Tables(tables) => {
                for table in tables {
                    node.field("table", table.name());
                }
            }
        }
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

    fn new_create_index(create_index: CreateIndex<'a>) -> Self {
        Self::CreateIndex(create_index)
    }

    fn new_drop(drop_object: DropObject<'a>) -> Self {
        Self::Drop(drop_object)
    }

    fn new_truncate(table: Table<'a>) -> Self {
        Self::Truncate(Truncate { table })
    }

    fn new_analyze(analyze: Analyze<'a>) -> Self {
        Self::Analyze(analyze)
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
        let mut constraints = HashSet::new();
        let mut columns = create_table.columns.clone();
        for constraint in create_table.constraints {
            match constraint {
                parser::Constraint::PrimaryKey(column_names) => {
                    if primary_keys.is_some() {
                        return Err(PlannerError::MultiplePrimaryKeys);
                    }
                    let column_indexes = column_indexes_from_names(&columns, &column_names)?;
                    primary_keys = Some(column_indexes.clone());
                    for column_index in &column_indexes {
                        columns[column_index.0].is_nullable = false;
                    }
                }
                parser::Constraint::Unique(column_names) => {
                    let column_indexes = column_indexes_from_names(&columns, &column_names)?;
                    constraints.insert(Constraint::Unique(column_indexes));
                }
            }
        }
        if primary_keys.is_none() {
            return Err(PlannerError::NoPrimaryKey);
        }
        let create_table = CreateTable {
            name: create_table.name,
            if_not_exists: create_table.if_not_exists,
            columns,
            primary_keys: primary_keys.unwrap(),
            constraints: constraints.into_iter().collect(),
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
            table,
            column_indexes,
            is_unique: create_index.is_unique,
        };
        Ok(PlanNode::new_create_index(create_index))
    }

    pub fn plan_drop(&self, drop_object: parser::DropObject) -> PlannerResult<PlanNode<'a>> {
        let catalog = self.ctx.catalog();
        let result = match drop_object.kind {
            parser::ObjectKind::Table => catalog.table(&drop_object.name).map(DropObject::Table),
            parser::ObjectKind::Index => {
                catalog
                    .index(&drop_object.name)
                    .map(|index| DropObject::Index {
                        table: index.table().clone(),
                        name: index.name().to_owned(),
                    })
            }
        };
        match result {
            Ok(drop_object) => Ok(PlanNode::new_drop(drop_object)),
            Err(CatalogError::UnknownEntry(_, _)) if drop_object.if_exists => {
                Ok(PlanNode::new_empty_row())
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn plan_truncate(&self, table_name: &str) -> PlannerResult<PlanNode<'a>> {
        let table = self.ctx.catalog().table(table_name)?;
        Ok(PlanNode::new_truncate(table))
    }

    pub fn plan_analyze(&self, analyze: parser::Analyze) -> PlannerResult<PlanNode<'a>> {
        let analyze = match analyze.table_names {
            Some(mut table_names) => {
                table_names.sort_unstable();
                table_names.dedup();
                let tables = table_names
                    .iter()
                    .map(|name| self.ctx.catalog().table(name))
                    .collect::<CatalogResult<_>>()?;
                Analyze::Tables(tables)
            }
            None => Analyze::AllTables,
        };
        Ok(PlanNode::new_analyze(analyze))
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

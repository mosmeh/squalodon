use super::{ColumnId, ExplainFormatter, Node, PlanNode, Planner, PlannerError, PlannerResult};
use crate::{
    catalog::{self, CatalogResult, Index, Sequence, Table},
    parser,
    rows::ColumnIndex,
    CatalogError,
};
use std::collections::HashSet;

#[derive(Clone)]
pub struct CreateTable {
    pub name: String,
    pub if_not_exists: bool,
    pub columns: Vec<catalog::Column>,
    pub primary_keys: Option<Vec<ColumnIndex>>,
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

    fn num_rows(&self) -> usize {
        0
    }

    fn cost(&self) -> f64 {
        PlanNode::DEFAULT_COST
    }
}

#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Constraint {
    Unique(Vec<ColumnIndex>),
}

#[derive(Clone)]
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

    fn num_rows(&self) -> usize {
        0
    }

    fn cost(&self) -> f64 {
        PlanNode::DEFAULT_COST
    }
}

#[derive(Clone)]
pub struct CreateSequence {
    pub name: String,
    pub if_not_exists: bool,
    pub increment_by: i64,
    pub min_value: i64,
    pub max_value: i64,
    pub start_value: i64,
    pub cycle: bool,
}

impl Node for CreateSequence {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("CreateSequence");
        node.field("name", &self.name)
            .field("increment by", self.increment_by)
            .field("min value", self.min_value)
            .field("max value", self.max_value)
            .field("start value", self.start_value)
            .field("cycle", self.cycle);
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}

    fn num_rows(&self) -> usize {
        0
    }

    fn cost(&self) -> f64 {
        PlanNode::DEFAULT_COST
    }
}

#[derive(Clone)]
pub enum DropObject<'a> {
    Table(Vec<Table<'a>>),
    Index(Vec<Index<'a>>),
    Sequence(Vec<Sequence<'a>>),
}

impl Node for DropObject<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Drop");
        match self {
            Self::Table(tables) => {
                for table in tables {
                    node.field("table", table.name());
                }
            }
            Self::Index(indexes) => {
                for index in indexes {
                    node.field("index", index.name());
                }
            }
            Self::Sequence(sequences) => {
                for sequence in sequences {
                    node.field("sequence", sequence.name());
                }
            }
        }
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}

    fn num_rows(&self) -> usize {
        0
    }

    fn cost(&self) -> f64 {
        PlanNode::DEFAULT_COST
    }
}

#[derive(Clone)]
pub struct Truncate<'a> {
    pub tables: Vec<Table<'a>>,
}

impl Node for Truncate<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        let mut node = f.node("Truncate");
        for table in &self.tables {
            node.field("table", table.name());
        }
    }

    fn append_outputs(&self, _: &mut Vec<ColumnId>) {}

    fn num_rows(&self) -> usize {
        0
    }

    fn cost(&self) -> f64 {
        PlanNode::DEFAULT_COST
    }
}

#[derive(Clone)]
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

    fn num_rows(&self) -> usize {
        0
    }

    fn cost(&self) -> f64 {
        PlanNode::DEFAULT_COST
    }
}

#[derive(Clone)]
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

    fn num_rows(&self) -> usize {
        0
    }

    fn cost(&self) -> f64 {
        PlanNode::DEFAULT_COST
    }
}

impl<'a> PlanNode<'a> {
    fn new_create_table(create_table: CreateTable) -> Self {
        Self::CreateTable(create_table)
    }

    fn new_create_index(create_index: CreateIndex<'a>) -> Self {
        Self::CreateIndex(create_index)
    }

    fn new_create_sequence(sequence: CreateSequence) -> Self {
        Self::CreateSequence(sequence)
    }

    fn new_drop(drop_object: DropObject<'a>) -> Self {
        Self::Drop(drop_object)
    }

    fn new_truncate(tables: Vec<Table<'a>>) -> Self {
        Self::Truncate(Truncate { tables })
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
            if catalog::Column::RESERVED_NAMES.contains(&column.name.as_str()) {
                return Err(PlannerError::ReservedName(column.name.clone()));
            }
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
        let create_table = CreateTable {
            name: create_table.name,
            if_not_exists: create_table.if_not_exists,
            columns,
            primary_keys,
            constraints: constraints.into_iter().collect(),
        };
        Ok(PlanNode::new_create_table(create_table))
    }

    pub fn plan_create_index(
        &self,
        create_index: parser::CreateIndex,
    ) -> PlannerResult<PlanNode<'a>> {
        let table = self.catalog.table(&create_index.table_name)?;
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

    #[allow(clippy::unused_self)]
    pub fn plan_create_sequence(
        &self,
        create_sequence: parser::CreateSequence,
    ) -> PlannerResult<PlanNode<'a>> {
        let increment_by = create_sequence.increment_by.unwrap_or(1);
        if increment_by == 0 {
            return Err(PlannerError::InvalidSequenceParameters);
        }
        let min_value =
            create_sequence
                .min_value
                .unwrap_or(if increment_by > 0 { 1 } else { i64::MIN });
        let max_value =
            create_sequence
                .max_value
                .unwrap_or(if increment_by > 0 { i64::MAX } else { -1 });
        if min_value >= max_value {
            return Err(PlannerError::InvalidSequenceParameters);
        }
        let start_value = create_sequence.start_value.unwrap_or({
            if increment_by > 0 {
                min_value
            } else {
                max_value
            }
        });
        if start_value < min_value || start_value > max_value {
            return Err(PlannerError::InvalidSequenceParameters);
        }
        let cycle = create_sequence.cycle.unwrap_or(false);
        Ok(PlanNode::new_create_sequence(CreateSequence {
            name: create_sequence.name,
            if_not_exists: create_sequence.if_not_exists,
            increment_by,
            min_value,
            max_value,
            start_value,
            cycle,
        }))
    }

    pub fn plan_drop(&self, drop_object: parser::DropObject) -> PlannerResult<PlanNode<'a>> {
        let result = match drop_object.kind {
            parser::ObjectKind::Table => drop_object
                .names
                .iter()
                .map(|name| self.catalog.table(name))
                .collect::<CatalogResult<Vec<_>>>()
                .map(DropObject::Table),
            parser::ObjectKind::Index => drop_object
                .names
                .iter()
                .map(|name| self.catalog.index(name))
                .collect::<CatalogResult<Vec<_>>>()
                .map(DropObject::Index),
            parser::ObjectKind::Sequence => drop_object
                .names
                .iter()
                .map(|name| self.catalog.sequence(name))
                .collect::<CatalogResult<Vec<_>>>()
                .map(DropObject::Sequence),
        };
        match result {
            Ok(drop_object) => Ok(PlanNode::new_drop(drop_object)),
            Err(CatalogError::UnknownEntry(_, _)) if drop_object.if_exists => {
                Ok(PlanNode::new_empty_row())
            }
            Err(err) => Err(err.into()),
        }
    }

    pub fn plan_truncate(&self, table_names: &[String]) -> PlannerResult<PlanNode<'a>> {
        let tables = table_names
            .iter()
            .map(|name| self.catalog.table(name))
            .collect::<CatalogResult<_>>()?;
        Ok(PlanNode::new_truncate(tables))
    }

    pub fn plan_analyze(&self, analyze: parser::Analyze) -> PlannerResult<PlanNode<'a>> {
        let analyze = match analyze.table_names {
            Some(mut table_names) => {
                table_names.sort_unstable();
                table_names.dedup();
                let tables = table_names
                    .iter()
                    .map(|name| self.catalog.table(name))
                    .collect::<CatalogResult<_>>()?;
                Analyze::Tables(tables)
            }
            None => Analyze::AllTables,
        };
        Ok(PlanNode::new_analyze(analyze))
    }

    pub fn plan_reindex(&self, reindex: parser::Reindex) -> PlannerResult<PlanNode<'a>> {
        let reindex = match reindex {
            parser::Reindex::Table(name) => Reindex::Table(self.catalog.table(&name)?),
            parser::Reindex::Index(name) => Reindex::Index(self.catalog.index(&name)?),
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

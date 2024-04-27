use super::{ExecutorNode, ExecutorResult};
use crate::{
    connection::ConnectionContext,
    planner::{Analyze, Constraint, CreateIndex, CreateTable, DropObject, Reindex, Truncate},
    CatalogError,
};

impl ExecutorNode<'_> {
    pub fn create_table(ctx: &ConnectionContext, plan: CreateTable) -> ExecutorResult<Self> {
        let CreateTable {
            name,
            if_not_exists,
            columns,
            primary_keys,
            constraints,
        } = plan;
        let result = ctx.catalog().create_table(&name, &columns, &primary_keys);
        match result {
            Ok(_) => (),
            Err(CatalogError::DuplicateEntry(_, _)) if if_not_exists => {
                return Ok(Self::empty_result())
            }
            Err(e) => return Err(e.into()),
        }
        let mut table = ctx.catalog().table(&name)?;
        for constraint in constraints {
            match constraint {
                Constraint::Unique(column_indexes) => {
                    let mut name = name.clone();
                    name.push('_');
                    for column_index in &column_indexes {
                        name.push_str(&columns[column_index.0].name);
                        name.push('_');
                    }
                    name.push_str("key");
                    table.create_index(name, &column_indexes, true)?;
                }
            }
        }
        Ok(Self::empty_result())
    }

    pub fn create_index(plan: CreateIndex) -> ExecutorResult<Self> {
        let CreateIndex {
            name,
            mut table,
            column_indexes,
            is_unique,
        } = plan;
        table.create_index(name, &column_indexes, is_unique)?;
        Ok(Self::empty_result())
    }

    pub fn drop_object(plan: DropObject) -> ExecutorResult<Self> {
        match plan {
            DropObject::Table(table) => table.drop_it()?,
            DropObject::Index { mut table, name } => table.drop_index(&name)?,
        }
        Ok(Self::empty_result())
    }

    pub fn truncate(plan: Truncate) -> ExecutorResult<Self> {
        let Truncate { table } = plan;
        table.truncate()?;
        Ok(Self::empty_result())
    }

    pub fn analyze(ctx: &ConnectionContext, plan: Analyze) -> ExecutorResult<Self> {
        match plan {
            Analyze::AllTables => {
                for table in ctx.catalog().tables() {
                    table?.analyze()?;
                }
            }
            Analyze::Tables(tables) => {
                for mut table in tables {
                    table.analyze()?;
                }
            }
        }
        Ok(Self::empty_result())
    }

    pub fn reindex(plan: Reindex) -> ExecutorResult<Self> {
        match plan {
            Reindex::Table(table) => table.reindex()?,
            Reindex::Index(index) => index.reindex()?,
        }
        Ok(Self::empty_result())
    }
}

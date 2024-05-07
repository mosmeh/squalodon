use super::{ExecutionContext, ExecutorNode, ExecutorResult};
use crate::{
    planner::{
        Analyze, Constraint, CreateIndex, CreateSequence, CreateTable, DropObject, Reindex,
        Truncate,
    },
    CatalogError,
};

impl ExecutorNode<'_> {
    pub fn create_table(ctx: &ExecutionContext, plan: CreateTable) -> ExecutorResult<Self> {
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

    pub fn create_sequence(ctx: &ExecutionContext, plan: CreateSequence) -> ExecutorResult<Self> {
        let CreateSequence {
            name,
            if_not_exists,
            increment_by,
            min_value,
            max_value,
            start_value,
            cycle,
        } = plan;
        let result = ctx.catalog().create_sequence(
            &name,
            increment_by,
            min_value,
            max_value,
            start_value,
            cycle,
        );
        match result {
            Ok(_) => (),
            Err(CatalogError::DuplicateEntry(_, _)) if if_not_exists => (),
            Err(e) => return Err(e.into()),
        }
        Ok(Self::empty_result())
    }

    pub fn drop_object(ctx: &ExecutionContext, plan: DropObject) -> ExecutorResult<Self> {
        match plan {
            DropObject::Table(tables) => {
                for table in tables {
                    table.drop_it()?;
                }
            }
            DropObject::Index(indexes) => {
                for index in indexes {
                    index.table().clone().drop_index(index.name())?;
                }
            }
            DropObject::Sequence(sequences) => {
                let names: Vec<_> = sequences.iter().map(|s| s.name().to_owned()).collect();
                for sequence in sequences {
                    sequence.drop_it()?;
                }
                // Remove only after al the sequences are successfully dropped
                for name in names {
                    ctx.local().borrow_mut().sequence_values.remove(&name);
                }
            }
        }
        Ok(Self::empty_result())
    }

    pub fn truncate(plan: Truncate) -> ExecutorResult<Self> {
        let Truncate { tables } = plan;
        for table in tables {
            table.truncate()?;
        }
        Ok(Self::empty_result())
    }

    pub fn analyze(ctx: &ExecutionContext, plan: Analyze) -> ExecutorResult<Self> {
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

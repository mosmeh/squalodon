use super::{ExecutionContext, ExecutorNode, ExecutorResult};
use crate::{
    catalog, parser,
    planner::{
        Analyze, Constraint, CreateIndex, CreateSequence, CreateTable, DropObject, Reindex,
        Truncate,
    },
    rows::ColumnIndex,
    CatalogError, Type,
};

impl ExecutorNode<'_> {
    pub fn create_table(ctx: &ExecutionContext, plan: CreateTable) -> ExecutorResult<Self> {
        let CreateTable {
            name,
            if_not_exists,
            mut columns,
            primary_keys,
            constraints,
        } = plan;
        let primary_keys = if let Some(p) = primary_keys {
            p
        } else {
            let sequence_name = rowid_sequence_name(&name);
            ctx.catalog()
                .create_sequence(&sequence_name, 1, 1, i64::MAX, 1, false)?;

            // Add a hidden column.
            let column_index = ColumnIndex(columns.len());
            let column = catalog::Column {
                name: catalog::Column::ROWID.to_owned(),
                ty: Type::Integer,
                is_nullable: false,
                default_value: parser::Expression::Function(parser::FunctionCall {
                    name: "nextval".to_owned(),
                    args: parser::FunctionArgs::Expressions(vec![parser::Expression::Constant(
                        sequence_name.into(),
                    )]),
                    is_distinct: false,
                })
                .into(),
            };
            assert!(column.is_hidden());
            // Add to the end to avoid changing the indexes of the other columns.
            columns.push(column);
            vec![column_index]
        };
        let result = ctx.catalog().create_table(&name, &columns, &primary_keys);
        let mut table = match result {
            Ok(table) => table,
            Err(CatalogError::DuplicateEntry(_, _)) if if_not_exists => {
                return Ok(Self::empty_result())
            }
            Err(e) => return Err(e.into()),
        };
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
                    if table
                        .columns()
                        .iter()
                        .any(|column| column.name == catalog::Column::ROWID)
                    {
                        match ctx.catalog.sequence(&rowid_sequence_name(table.name())) {
                            Ok(sequence) => sequence.drop_it()?,
                            Err(CatalogError::UnknownEntry(_, _)) => {
                                // User dropped the sequence manually.
                            }
                            Err(e) => return Err(e.into()),
                        }
                    }

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

fn rowid_sequence_name(table_name: &str) -> String {
    format!("{}_{}_seq", table_name, catalog::Column::ROWID)
}

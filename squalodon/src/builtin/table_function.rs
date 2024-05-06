use crate::{
    catalog::{BoxedTableFn, Function, TableFunction},
    lexer,
    planner::Column,
    Row, Type,
};
use std::collections::HashSet;

pub fn load() -> impl Iterator<Item = TableFunction> {
    [
        TableFunction {
            name: "generate_series",
            argument_types: &[Type::Integer, Type::Integer],
            result_columns: vec![Column::new("generate_series", Type::Integer)],
            eval: BoxedTableFn::new(|_, (start, end): (i64, i64)| {
                Ok((start..=end).map(|i| Row::new(vec![i.into()])))
            }),
        },
        TableFunction {
            name: "squalodon_columns",
            argument_types: &[],
            result_columns: vec![
                Column::new("table_name", Type::Text),
                Column::new("column_name", Type::Text),
                Column::new("type", Type::Text),
                Column::new("is_nullable", Type::Boolean),
                Column::new("is_primary_key", Type::Boolean),
                Column::new("default_value", Type::Text),
            ],
            eval: BoxedTableFn::new(|ctx, ()| {
                let mut rows = Vec::new();
                for table in ctx.catalog().tables() {
                    let table = table?;
                    let mut is_primary_key = vec![false; table.columns().len()];
                    for column_index in table.primary_keys() {
                        is_primary_key[column_index.0] = true;
                    }
                    for (column, is_primary_key) in table.columns().iter().zip(is_primary_key) {
                        let default_value = column
                            .default_value
                            .as_ref()
                            .map_or_else(String::new, ToString::to_string)
                            .into();
                        rows.push(Row::new(vec![
                            table.name().into(),
                            column.name.clone().into(),
                            column.ty.to_string().into(),
                            column.is_nullable.into(),
                            is_primary_key.into(),
                            default_value,
                        ]));
                    }
                }
                Ok(rows.into_iter())
            }),
        },
        TableFunction {
            name: "squalodon_functions",
            argument_types: &[],
            result_columns: vec![
                Column::new("name", Type::Text),
                Column::new("type", Type::Text),
            ],
            eval: BoxedTableFn::new(|ctx, ()| {
                let mut dedup_set = HashSet::new();
                for function in ctx.catalog().functions() {
                    let kind = match function {
                        Function::Scalar(_) => "scalar",
                        Function::Aggregate(_) => "aggregate",
                        Function::Table(_) => "table",
                    };
                    dedup_set.insert((function.name().to_owned(), kind));
                }
                let rows = dedup_set
                    .into_iter()
                    .map(|(name, kind)| Row::new(vec![name.into(), kind.into()]));
                Ok(rows)
            }),
        },
        TableFunction {
            name: "squalodon_indexes",
            argument_types: &[],
            result_columns: vec![
                Column::new("table_name", Type::Text),
                Column::new("index_name", Type::Text),
                Column::new("is_unique", Type::Boolean),
            ],
            eval: BoxedTableFn::new(|ctx, ()| {
                let mut rows = Vec::new();
                for table in ctx.catalog().tables() {
                    let table = table?;
                    for index in table.indexes() {
                        rows.push(Row::new(vec![
                            table.name().into(),
                            index.name().into(),
                            index.is_unique().into(),
                        ]));
                    }
                }
                Ok(rows.into_iter())
            }),
        },
        TableFunction {
            name: "squalodon_keywords",
            argument_types: &[],
            result_columns: vec![Column::new("keyword", Type::Text)],
            eval: BoxedTableFn::new(|_, ()| {
                let rows = lexer::KEYWORDS
                    .iter()
                    .map(|keyword| Row::new(vec![keyword.to_ascii_uppercase().into()]));
                Ok(rows)
            }),
        },
        TableFunction {
            name: "squalodon_sequences",
            argument_types: &[],
            result_columns: vec![
                Column::new("name", Type::Text),
                Column::new("increment_by", Type::Integer),
                Column::new("min_value", Type::Integer),
                Column::new("max_value", Type::Integer),
                Column::new("start_value", Type::Integer),
                Column::new("cycle", Type::Boolean),
            ],
            eval: BoxedTableFn::new(|ctx, ()| {
                let mut rows = Vec::new();
                for sequence in ctx.catalog().sequences() {
                    let sequence = sequence?;
                    rows.push(Row::new(vec![
                        sequence.name().into(),
                        sequence.increment_by().into(),
                        sequence.min_value().into(),
                        sequence.max_value().into(),
                        sequence.start_value().into(),
                        sequence.cycle().into(),
                    ]));
                }
                Ok(rows.into_iter())
            }),
        },
        TableFunction {
            name: "squalodon_tables",
            argument_types: &[],
            result_columns: vec![Column::new("name", Type::Text)],
            eval: BoxedTableFn::new(|ctx, ()| {
                let mut rows = Vec::new();
                for table in ctx.catalog().tables() {
                    let table = table?;
                    rows.push(Row::new(vec![table.name().into()]));
                }
                Ok(rows.into_iter())
            }),
        },
    ]
    .into_iter()
}

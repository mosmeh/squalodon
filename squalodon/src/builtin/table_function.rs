use crate::{
    catalog::{Function, TableFunction},
    lexer,
    planner::Column,
    Row, Type,
};

pub fn load() -> impl Iterator<Item = TableFunction> {
    [
        TableFunction {
            name: "squalodon_columns",
            fn_ptr: |ctx, _| {
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
                Ok(Box::new(rows.into_iter()))
            },
            result_columns: vec![
                Column::new("table_name", Type::Text),
                Column::new("column_name", Type::Text),
                Column::new("type", Type::Text),
                Column::new("is_nullable", Type::Boolean),
                Column::new("is_primary_key", Type::Boolean),
                Column::new("default_value", Type::Text),
            ],
        },
        TableFunction {
            name: "squalodon_functions",
            fn_ptr: |ctx, _| {
                let rows = ctx.catalog().functions().map(|function| {
                    let kind = match function {
                        Function::Scalar(_) => "scalar",
                        Function::Aggregate(_) => "aggregate",
                        Function::Table(_) => "table",
                    };
                    Row::new(vec![function.name().into(), kind.into()])
                });
                Ok(Box::new(rows))
            },
            result_columns: vec![
                Column::new("name", Type::Text),
                Column::new("type", Type::Text),
            ],
        },
        TableFunction {
            name: "squalodon_indexes",
            fn_ptr: |ctx, _| {
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
                Ok(Box::new(rows.into_iter()))
            },
            result_columns: vec![
                Column::new("table_name", Type::Text),
                Column::new("index_name", Type::Text),
                Column::new("is_unique", Type::Boolean),
            ],
        },
        TableFunction {
            name: "squalodon_keywords",
            fn_ptr: |_, _| {
                let rows = lexer::KEYWORDS
                    .iter()
                    .map(|keyword| Row::new(vec![keyword.to_ascii_uppercase().into()]));
                Ok(Box::new(rows))
            },
            result_columns: vec![Column::new("keyword", Type::Text)],
        },
        TableFunction {
            name: "squalodon_tables",
            fn_ptr: |ctx, _| {
                let mut rows = Vec::new();
                for table in ctx.catalog().tables() {
                    let table = table?;
                    rows.push(Row::new(vec![table.name().into()]));
                }
                Ok(Box::new(rows.into_iter()))
            },
            result_columns: vec![Column::new("name", Type::Text)],
        },
    ]
    .into_iter()
}

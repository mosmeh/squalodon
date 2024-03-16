use crate::{catalog::TableFunction, planner::Column, storage::KeyValueStore, Type};

pub fn load_table_functions<T: KeyValueStore>() -> impl Iterator<Item = (String, TableFunction<T>)>
{
    [
        (
            "squalodon_tables",
            TableFunction {
                fn_ptr: |ctx, _| {
                    let mut rows = Vec::new();
                    for table in ctx.catalog().tables() {
                        let table = table?;
                        rows.push(vec![table.name.into()]);
                    }
                    Ok(Box::new(rows.into_iter()))
                },
                result_columns: vec![Column::new("name", Type::Text)],
            },
        ),
        (
            "squalodon_columns",
            TableFunction {
                fn_ptr: |ctx, _| {
                    let mut rows = Vec::new();
                    for table in ctx.catalog().tables() {
                        let table = table?;
                        for column in table.columns {
                            rows.push(vec![
                                table.name.clone().into(),
                                column.name.into(),
                                column.ty.to_string().into(),
                                column.is_nullable.into(),
                                column.is_primary_key.into(),
                            ]);
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
                ],
            },
        ),
    ]
    .into_iter()
    .map(|(name, f)| (name.to_owned(), f))
}

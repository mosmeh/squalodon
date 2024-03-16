use crate::{catalog::TableFunction, planner::Column, storage::KeyValueStore, Type};

pub fn load_table_functions<T: KeyValueStore>() -> impl Iterator<Item = (String, TableFunction<T>)>
{
    std::iter::once((
        "squalodon_tables".to_owned(),
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
    ))
}

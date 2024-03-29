use crate::{
    catalog::{Constraint, TableFunction},
    planner::Column,
    storage::Storage,
    Row, Type,
};

pub fn load_table_functions<T: Storage>() -> impl Iterator<Item = (String, TableFunction<T>)> {
    [
        (
            "squalodon_tables",
            TableFunction {
                fn_ptr: |ctx, _| {
                    let mut rows = Vec::new();
                    for table in ctx.catalog().tables() {
                        let table = table?;
                        rows.push(Row(vec![table.name().into()]));
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
                    #[derive(Clone, Default)]
                    struct ColumnConstraint {
                        is_nullable: bool,
                        is_primary_key: bool,
                    }
                    let mut rows = Vec::new();
                    for table in ctx.catalog().tables() {
                        let table = table?;
                        let mut constraints =
                            vec![ColumnConstraint::default(); table.columns().len()];
                        for constraint in table.constraints() {
                            match constraint {
                                Constraint::NotNull(column) => {
                                    constraints[column.0].is_nullable = false;
                                }
                                Constraint::PrimaryKey(columns) => {
                                    for column in columns {
                                        constraints[column.0].is_primary_key = true;
                                    }
                                }
                            }
                        }
                        for (column, constraint) in table.columns().iter().zip(constraints) {
                            rows.push(Row(vec![
                                table.name().into(),
                                column.name.clone().into(),
                                column.ty.to_string().into(),
                                constraint.is_nullable.into(),
                                constraint.is_primary_key.into(),
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
                ],
            },
        ),
    ]
    .into_iter()
    .map(|(name, f)| (name.to_owned(), f))
}

use sqllogictest::{
    harness::{self, Arguments, Trial},
    strict_column_validator, DBOutput, Runner,
};
use squalodon::{storage::Memory, Error, Type};

fn main() {
    let paths = harness::glob("tests/slt/**/*.slt").expect("failed to find test files");
    let tests: Vec<_> = paths
        .map(|entry| {
            let path = entry.expect("failed to read glob entry");
            let name = path.display().to_string();
            Trial::test(name, move || {
                let mut tester = Runner::new(|| async { Ok(Database::default()) });
                tester.with_column_validator(strict_column_validator);
                tester.run_file(&path)?;
                Ok(())
            })
        })
        .collect();
    assert!(!tests.is_empty(), "no sqllogictest found");
    harness::run(&Arguments::from_args(), tests).exit()
}

#[derive(Default)]
struct Database(squalodon::Database<Memory>);

impl sqllogictest::DB for Database {
    type Error = Error;
    type ColumnType = ColumnType;

    fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let rows = self.0.connect().query(sql, [])?;
        let types = rows
            .columns()
            .iter()
            .map(|column| ColumnType(column.ty()))
            .collect();
        let rows = rows
            .into_iter()
            .map(|row| row.columns().iter().map(ToString::to_string).collect())
            .collect();
        Ok(DBOutput::Rows { types, rows })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ColumnType(Type);

impl sqllogictest::ColumnType for ColumnType {
    fn from_char(value: char) -> Option<Self> {
        Some(Self(match value {
            'I' => Type::Integer,
            'R' => Type::Real,
            'B' => Type::Boolean,
            'T' => Type::Text,
            _ => return None,
        }))
    }

    fn to_char(&self) -> char {
        match self.0 {
            Type::Integer => 'I',
            Type::Real => 'R',
            Type::Boolean => 'B',
            Type::Text => 'T',
        }
    }
}

use super::{ExecutorNode, ExecutorResult, IntoOutput, Node, Output};
use crate::{catalog::Table, storage::Transaction, Row};

pub struct Insert {
    row: Option<Row>,
}

impl Insert {
    pub fn new<T: Transaction>(
        source: ExecutorNode<'_, T>,
        table: Table<'_, T>,
    ) -> ExecutorResult<Self> {
        let mut count = 0;
        for row in source {
            table.insert(&row?)?;
            count += 1;
        }
        Ok(Self {
            row: Some(Row::new(vec![count.into()])),
        })
    }
}

impl Node for Insert {
    fn next_row(&mut self) -> Output {
        self.row.take().into_output()
    }
}

pub struct Update {
    row: Option<Row>,
}

impl Update {
    pub fn new<T: Transaction>(
        source: ExecutorNode<'_, T>,
        table: Table<'_, T>,
    ) -> ExecutorResult<Self> {
        let num_columns = table.columns().len();
        let mut count = 0;
        for row in source {
            let row = row?;
            assert_eq!(row.0.len(), 2 * num_columns);
            let (old_row, new_row) = row.0.split_at(num_columns);
            table.update(old_row, new_row)?;
            count += 1;
        }
        Ok(Self {
            row: Some(Row::new(vec![count.into()])),
        })
    }
}

impl Node for Update {
    fn next_row(&mut self) -> Output {
        self.row.take().into_output()
    }
}

pub struct Delete {
    row: Option<Row>,
}

impl Delete {
    pub fn new<T: Transaction>(
        source: ExecutorNode<'_, T>,
        table: Table<'_, T>,
    ) -> ExecutorResult<Self> {
        let mut count = 0;
        for row in source {
            table.delete(&row?)?;
            count += 1;
        }
        Ok(Self {
            row: Some(Row::new(vec![count.into()])),
        })
    }
}

impl Node for Delete {
    fn next_row(&mut self) -> Output {
        self.row.take().into_output()
    }
}

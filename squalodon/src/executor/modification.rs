use super::{ExecutorNode, ExecutorResult, IntoOutput, Node, Output};
use crate::{storage::Table, Row, Storage};

pub struct Insert {
    row: Option<Row>,
}

impl Insert {
    pub fn new<T: Storage>(
        source: Box<ExecutorNode<'_, '_, T>>,
        table: Table<'_, '_, T>,
    ) -> ExecutorResult<Self> {
        let mut count = 0;
        for row in source {
            table.insert(&row?)?;
            count += 1;
        }
        Ok(Self {
            row: Some(Row(vec![count.into()])),
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
    pub fn new<T: Storage>(
        source: Box<ExecutorNode<'_, '_, T>>,
        table: Table<'_, '_, T>,
    ) -> ExecutorResult<Self> {
        let mut count = 0;
        for row in source {
            table.update(&row?)?;
            count += 1;
        }
        Ok(Self {
            row: Some(Row(vec![count.into()])),
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
    pub fn new<T: Storage>(
        source: Box<ExecutorNode<'_, '_, T>>,
        table: Table<'_, '_, T>,
    ) -> ExecutorResult<Self> {
        let mut count = 0;
        for row in source {
            table.delete(&row?)?;
            count += 1;
        }
        Ok(Self {
            row: Some(Row(vec![count.into()])),
        })
    }
}

impl Node for Delete {
    fn next_row(&mut self) -> Output {
        self.row.take().into_output()
    }
}

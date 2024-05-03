use super::{ExecutionContext, ExecutorNode, ExecutorResult, IntoOutput, Node, Output};
use crate::{catalog::Table, planner, Row};

pub struct Insert {
    row: Option<Row>,
}

impl Insert {
    fn new(source: ExecutorNode, table: Table) -> ExecutorResult<Self> {
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
    fn new(source: ExecutorNode, table: Table) -> ExecutorResult<Self> {
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
    fn new(source: ExecutorNode, table: Table) -> ExecutorResult<Self> {
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

impl<'a> ExecutorNode<'a> {
    pub fn insert(ctx: &'a ExecutionContext, plan: planner::Insert<'a>) -> ExecutorResult<Self> {
        let planner::Insert { source, table, .. } = plan;
        Ok(Self::Insert(Insert::new(Self::new(ctx, *source)?, table)?))
    }

    pub fn update(ctx: &'a ExecutionContext, plan: planner::Update<'a>) -> ExecutorResult<Self> {
        let planner::Update { source, table, .. } = plan;
        Ok(Self::Update(Update::new(Self::new(ctx, *source)?, table)?))
    }

    pub fn delete(ctx: &'a ExecutionContext, plan: planner::Delete<'a>) -> ExecutorResult<Self> {
        let planner::Delete { source, table, .. } = plan;
        Ok(Self::Delete(Delete::new(Self::new(ctx, *source)?, table)?))
    }
}

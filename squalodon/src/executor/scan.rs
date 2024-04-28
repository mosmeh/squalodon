use super::{ConnectionContext, ExecutorNode, ExecutorResult, IntoOutput, Node, Output};
use crate::{
    catalog::{Index, Table, TableFnPtr},
    planner::{ExecutableExpression, Scan},
    storage::StorageResult,
    Row, Value,
};
use std::ops::RangeBounds;

pub struct SeqScan<'a> {
    iter: Box<dyn Iterator<Item = StorageResult<Row>> + 'a>,
}

impl<'a> SeqScan<'a> {
    fn new(table: Table<'a>) -> Self {
        Self {
            iter: Box::new(table.scan()),
        }
    }
}

impl Node for SeqScan<'_> {
    fn next_row(&mut self) -> Output {
        self.iter.next().into_output()
    }
}

pub struct IndexScan<'a> {
    iter: Box<dyn Iterator<Item = StorageResult<Row>> + 'a>,
}

impl<'a> IndexScan<'a> {
    fn new<'r, R: RangeBounds<&'r [Value]>>(index: Index<'a>, range: R) -> Self {
        Self {
            iter: Box::new(index.scan(range)),
        }
    }
}

impl Node for IndexScan<'_> {
    fn next_row(&mut self) -> Output {
        self.iter.next().into_output()
    }
}

pub struct IndexOnlyScan<'a> {
    iter: Box<dyn Iterator<Item = StorageResult<Row>> + 'a>,
}

impl<'a> IndexOnlyScan<'a> {
    fn new<'r, R: RangeBounds<&'r [Value]>>(index: Index<'a>, range: R) -> Self {
        Self {
            iter: Box::new(index.scan_index_only(range).map(|r| r.map(Row::new))),
        }
    }
}

impl Node for IndexOnlyScan<'_> {
    fn next_row(&mut self) -> Output {
        self.iter.next().into_output()
    }
}

pub struct FunctionScan<'a> {
    ctx: &'a ConnectionContext<'a>,
    source: Box<ExecutorNode<'a>>,
    fn_ptr: TableFnPtr,
    rows: Box<dyn Iterator<Item = Row> + 'a>,
}

impl<'a> FunctionScan<'a> {
    fn new(ctx: &'a ConnectionContext, source: ExecutorNode<'a>, fn_ptr: TableFnPtr) -> Self {
        Self {
            ctx,
            source: source.into(),
            fn_ptr,
            rows: Box::new(std::iter::empty()),
        }
    }
}

impl Node for FunctionScan<'_> {
    fn next_row(&mut self) -> Output {
        loop {
            if let Some(row) = self.rows.next() {
                return Ok(row);
            }
            let row = self.source.next_row()?;
            self.rows = (self.fn_ptr)(self.ctx, &row)?;
        }
    }
}

pub struct ExpressionScan<'a> {
    rows: Box<dyn Iterator<Item = ExecutorResult<Row>> + 'a>,
}

impl<'a> ExpressionScan<'a> {
    pub fn new(ctx: &'a ConnectionContext<'a>, rows: Vec<Vec<ExecutableExpression<'a>>>) -> Self {
        let rows = rows.into_iter().map(|row| {
            let columns = row
                .into_iter()
                .map(|expr| expr.eval(ctx, &Row::empty()))
                .collect::<ExecutorResult<_>>()?;
            Ok(Row(columns))
        });
        Self {
            rows: Box::new(rows),
        }
    }

    pub fn no_rows() -> Self {
        Self {
            rows: Box::new(std::iter::empty()),
        }
    }
}

impl Node for ExpressionScan<'_> {
    fn next_row(&mut self) -> Output {
        self.rows.next().into_output()
    }
}

impl<'a> ExecutorNode<'a> {
    pub fn scan(ctx: &'a ConnectionContext, plan: Scan<'a>) -> ExecutorResult<Self> {
        match plan {
            Scan::Seq { table, .. } => Ok(Self::SeqScan(SeqScan::new(table))),
            Scan::Index { index, range, .. } => {
                let range = (
                    range.0.as_ref().map(Vec::as_slice),
                    range.1.as_ref().map(Vec::as_slice),
                );
                Ok(Self::IndexScan(IndexScan::new(index, range)))
            }
            Scan::IndexOnly { index, range, .. } => {
                let range = (
                    range.0.as_ref().map(Vec::as_slice),
                    range.1.as_ref().map(Vec::as_slice),
                );
                Ok(Self::IndexOnlyScan(IndexOnlyScan::new(index, range)))
            }
            Scan::Function {
                source, function, ..
            } => Ok(Self::FunctionScan(FunctionScan::new(
                ctx,
                Self::new(ctx, *source)?,
                function.fn_ptr,
            ))),
            Scan::Expression { rows, .. } => {
                let rows = rows
                    .into_iter()
                    .map(|row| {
                        row.into_iter()
                            .map(|expr| expr.into_executable(&[]))
                            .collect()
                    })
                    .collect();
                Ok(Self::ExpressionScan(ExpressionScan::new(ctx, rows)))
            }
        }
    }
}

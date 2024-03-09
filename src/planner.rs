mod binder;

use crate::{
    parser,
    storage::{Catalog, TableId},
    types::{Type, Value},
    BinaryOp, NullOrder, Order, StorageError,
};
use std::rc::Rc;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Unknown column {0:?}")]
    UnknownColumn(String),

    #[error("Type error")]
    TypeError,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

type Result<T> = std::result::Result<T, Error>;

pub fn plan(catalog: &Catalog, statement: parser::Statement) -> Result<PlanNode> {
    binder::Binder::new(catalog).bind(statement)
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnIndex(pub usize);

impl std::fmt::Display for ColumnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

#[derive(Debug)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    fn empty() -> Self {
        Self {
            columns: Vec::new(),
        }
    }

    fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    fn resolve_column(&self, name: &str) -> Result<(ColumnIndex, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == name)
            .map(|(i, column)| (ColumnIndex(i), column))
            .ok_or(Error::UnknownColumn(name.to_owned()))
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub ty: Option<Type>,
}

pub struct PlanNode {
    pub kind: PlanKind,
    pub schema: Rc<Schema>,
}

impl PlanNode {
    fn empty_row() -> Self {
        Self {
            kind: PlanKind::OneRow(OneRow {
                columns: Vec::new(),
            }),
            schema: Schema::empty().into(),
        }
    }
}

trait Explain {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result;

    fn explain_child(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        let depth = depth + 1;
        for _ in 0..depth {
            f.write_str("  ")?;
        }
        self.explain(f, depth)
    }
}

impl Explain for PlanNode {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        match &self.kind {
            PlanKind::Explain(explain) => explain.explain(f, depth),
            PlanKind::CreateTable(create_table) => create_table.explain(f, depth),
            PlanKind::Insert(insert) => insert.explain(f, depth),
            PlanKind::Scan(scan) => scan.explain(f, depth),
            PlanKind::Project(project) => project.explain(f, depth),
            PlanKind::Filter(filter) => filter.explain(f, depth),
            PlanKind::Sort(sort) => sort.explain(f, depth),
            PlanKind::Limit(limit) => limit.explain(f, depth),
            PlanKind::OneRow(one_row) => one_row.explain(f, depth),
        }
    }
}

impl std::fmt::Display for PlanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.explain(f, 0)
    }
}

pub enum PlanKind {
    Explain(Box<PlanNode>),
    CreateTable(CreateTable),
    Insert(Insert),
    Scan(Scan),
    Project(Project),
    Filter(Filter),
    Sort(Sort),
    Limit(Limit),
    OneRow(OneRow),
}

pub struct CreateTable(pub parser::CreateTable);

impl Explain for CreateTable {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, _: usize) -> std::fmt::Result {
        write!(f, "CreateTable {:?}", self.0)
    }
}

pub struct Insert {
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
    pub exprs: Vec<Expression>,
}

impl Explain for Insert {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, _: usize) -> std::fmt::Result {
        write!(f, "Insert table={} exprs={:?}", self.table.0, self.exprs)
    }
}

#[derive(Debug)]
pub enum Expression {
    Constact(Value),
    ColumnRef {
        column: ColumnIndex,
    },
    BinaryOp {
        op: BinaryOp,
        lhs: Box<Expression>,
        rhs: Box<Expression>,
    },
}

impl std::fmt::Display for Expression {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Constact(value) => write!(f, "{value:?}"),
            Self::ColumnRef { column } => write!(f, "{column}"),
            Self::BinaryOp { op, lhs, rhs } => {
                write!(f, "({lhs} {op} {rhs})")
            }
        }
    }
}

#[derive(Debug)]
pub enum Scan {
    SeqScan {
        table: TableId,
        columns: Vec<ColumnIndex>,
    },
}

impl Explain for Scan {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, _: usize) -> std::fmt::Result {
        match self {
            Self::SeqScan { table, columns } => {
                write!(f, "SeqScan table={}, columns=[", table.0)?;
                for (i, column) in columns.iter().enumerate() {
                    if i == 0 {
                        write!(f, "{column}")?;
                    } else {
                        write!(f, ", {column}")?;
                    }
                }
                f.write_str("]")
            }
        }
    }
}

pub struct Project {
    pub source: Box<PlanNode>,
    pub exprs: Vec<Expression>,
}

impl Explain for Project {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        write!(f, "Project ")?;
        for (i, expr) in self.exprs.iter().enumerate() {
            if i == 0 {
                write!(f, "{expr}")?;
            } else {
                write!(f, ", {expr}")?;
            }
        }
        f.write_str("\n")?;
        self.source.explain_child(f, depth)
    }
}

pub struct Filter {
    pub source: Box<PlanNode>,
    pub cond: Expression,
}

impl Explain for Filter {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        writeln!(f, "Filter {}", self.cond)?;
        self.source.explain_child(f, depth)
    }
}

pub struct Sort {
    pub source: Box<PlanNode>,
    pub order_by: Vec<OrderBy>,
}

impl Explain for Sort {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        f.write_str("Sort by [")?;
        for (i, order_by) in self.order_by.iter().enumerate() {
            if i == 0 {
                write!(f, "{order_by}")?;
            } else {
                write!(f, ", {order_by}")?;
            }
        }
        f.write_str("]\n")?;
        self.source.explain_child(f, depth)
    }
}

pub struct Limit {
    pub source: Box<PlanNode>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

impl Explain for Limit {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        f.write_str("Limit")?;
        if let Some(limit) = &self.limit {
            write!(f, " limit={limit}")?;
        }
        if let Some(offset) = &self.offset {
            write!(f, " offset={offset}")?;
        }
        f.write_str("\n")?;
        self.source.explain_child(f, depth)
    }
}

pub struct OrderBy {
    pub expr: Expression,
    pub order: Order,
    pub null_order: NullOrder,
}

impl std::fmt::Display for OrderBy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} {} {}", self.expr, self.order, self.null_order)
    }
}

pub struct OneRow {
    pub columns: Vec<Expression>,
}

impl Explain for OneRow {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, _: usize) -> std::fmt::Result {
        write!(f, "OneRow {:?}", self.columns)
    }
}

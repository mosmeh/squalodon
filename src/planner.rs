mod binder;

use crate::{
    parser,
    storage::{self, TableId, Transaction},
    types::{Type, Value},
    BinaryOp, KeyValueStore, NullOrder, Order, StorageError, UnaryOp,
};
use std::fmt::Write;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Table {0:?} already exists")]
    TableAlreadyExists(String),

    #[error("Unknown column {0:?}")]
    UnknownColumn(String),

    #[error("Duplicate column {0:?}")]
    DuplicateColumn(String),

    #[error("Type error")]
    TypeError,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),
}

type Result<T> = std::result::Result<T, Error>;

pub fn plan<T: KeyValueStore>(
    txn: &Transaction<T>,
    statement: parser::Statement,
) -> Result<TypedPlanNode> {
    binder::Binder::new(txn).bind(statement)
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnIndex(pub usize);

impl std::fmt::Display for ColumnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

pub struct TypedPlanNode {
    node: PlanNode,
    columns: Vec<Column>,
}

impl TypedPlanNode {
    pub fn into_node(self) -> PlanNode {
        self.node
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    fn empty_source() -> Self {
        Self {
            node: PlanNode::Values(Values::one_empty_row()),
            columns: Vec::new(),
        }
    }

    /// Creates a node that does not produce any rows.
    fn sink(node: PlanNode) -> Self {
        Self {
            node,
            columns: Vec::new(),
        }
    }

    fn inherit_schema<F>(self, f: F) -> Self
    where
        F: FnOnce(PlanNode) -> PlanNode,
    {
        Self {
            node: f(self.node),
            columns: self.columns,
        }
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

trait Explain {
    fn visit(&self, visitor: &mut ExplainVisitor);
}

#[derive(Default)]
struct ExplainVisitor {
    rows: Vec<String>,
    depth: isize,
}

impl ExplainVisitor {
    fn write_str(&mut self, s: &str) {
        let mut row = String::new();
        for _ in 0..(self.depth - 1) {
            row.push_str("  ");
        }
        row.push_str(s);
        self.rows.push(row);
    }

    fn write_fmt(&mut self, fmt: std::fmt::Arguments) {
        self.write_str(&fmt.to_string());
    }
}

pub enum PlanNode {
    Explain(Box<PlanNode>),
    CreateTable(CreateTable),
    DropTable(DropTable),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Values(Values),
    Scan(Scan),
    Project(Project),
    Filter(Filter),
    Sort(Sort),
    Limit(Limit),
}

impl PlanNode {
    pub fn explain(&self) -> Vec<String> {
        let mut visitor = ExplainVisitor::default();
        self.visit(&mut visitor);
        visitor.rows
    }
}

impl Explain for PlanNode {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        visitor.depth += 1;
        match self {
            Self::Explain(n) => n.visit(visitor),
            Self::CreateTable(n) => n.visit(visitor),
            Self::DropTable(n) => n.visit(visitor),
            Self::Insert(n) => n.visit(visitor),
            Self::Update(n) => n.visit(visitor),
            Self::Delete(n) => n.visit(visitor),
            Self::Values(n) => n.visit(visitor),
            Self::Scan(n) => n.visit(visitor),
            Self::Project(n) => n.visit(visitor),
            Self::Filter(n) => n.visit(visitor),
            Self::Sort(n) => n.visit(visitor),
            Self::Limit(n) => n.visit(visitor),
        }
        visitor.depth -= 1;
    }
}

pub struct CreateTable {
    pub name: String,
    pub columns: Vec<storage::Column>,
}

impl Explain for CreateTable {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "CreateTable {:?}", self.name);
    }
}

pub struct DropTable {
    pub name: String,
}

impl Explain for DropTable {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "DropTable {:?}", self.name);
    }
}

pub struct Insert {
    pub source: Box<PlanNode>,
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
}

impl Explain for Insert {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Insert table={:?}", self.table);
        self.source.visit(visitor);
    }
}

pub struct Update {
    pub source: Box<PlanNode>,
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
}

impl Explain for Update {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Update table={:?}", self.table);
        self.source.visit(visitor);
    }
}

pub struct Delete {
    pub source: Box<PlanNode>,
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
}

impl Explain for Delete {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Delete table={:?}", self.table);
        self.source.visit(visitor);
    }
}

#[derive(Debug, Clone)]
pub enum Expression {
    Constact(Value),
    ColumnRef {
        column: ColumnIndex,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expression>,
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
            Self::UnaryOp { op, expr } => write!(f, "({op} {expr})"),
            Self::BinaryOp { op, lhs, rhs } => {
                write!(f, "({lhs} {op} {rhs})")
            }
        }
    }
}

pub struct Values {
    pub rows: Vec<Vec<Expression>>,
}

impl Values {
    fn new(rows: Vec<Vec<Expression>>) -> Self {
        Self { rows }
    }

    fn one_empty_row() -> Self {
        Self::new(vec![Vec::new()])
    }
}

impl Explain for Values {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut f = "Values ".to_owned();
        for (i, row) in self.rows.iter().enumerate() {
            f.push_str(if i == 0 { "[" } else { ", [" });
            for (j, value) in row.iter().enumerate() {
                if j == 0 {
                    write!(&mut f, "{value:?}").unwrap();
                } else {
                    write!(&mut f, ", {value:?}").unwrap();
                }
            }
            f.push(']');
        }
        visitor.write_str(&f);
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
    fn visit(&self, visitor: &mut ExplainVisitor) {
        match self {
            Self::SeqScan { table, columns } => {
                let mut f = String::new();
                write!(&mut f, "SeqScan table={}, columns=[", table.0).unwrap();
                for (i, column) in columns.iter().enumerate() {
                    if i == 0 {
                        write!(&mut f, "{column}").unwrap();
                    } else {
                        write!(&mut f, ", {column}").unwrap();
                    }
                }
                f.push(']');
                visitor.write_str(&f);
            }
        }
    }
}

pub struct Project {
    pub source: Box<PlanNode>,
    pub exprs: Vec<Expression>,
}

impl Explain for Project {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut f = "Project ".to_owned();
        for (i, expr) in self.exprs.iter().enumerate() {
            if i == 0 {
                write!(f, "{expr}").unwrap();
            } else {
                write!(f, ", {expr}").unwrap();
            }
        }
        visitor.write_str(&f);
        self.source.visit(visitor);
    }
}

pub struct Filter {
    pub source: Box<PlanNode>,
    pub cond: Expression,
}

impl Explain for Filter {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        writeln!(visitor, "Filter {}", self.cond);
        self.source.visit(visitor);
    }
}

pub struct Sort {
    pub source: Box<PlanNode>,
    pub order_by: Vec<OrderBy>,
}

impl Explain for Sort {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut f = "Sort by [".to_owned();
        for (i, order_by) in self.order_by.iter().enumerate() {
            if i == 0 {
                write!(f, "{order_by}").unwrap();
            } else {
                write!(f, ", {order_by}").unwrap();
            }
        }
        f.push(']');
        visitor.write_str(&f);
        self.source.visit(visitor);
    }
}

pub struct Limit {
    pub source: Box<PlanNode>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

impl Explain for Limit {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        let mut f = "Limit".to_owned();
        if let Some(limit) = &self.limit {
            write!(f, " limit={limit}").unwrap();
        }
        if let Some(offset) = &self.offset {
            write!(f, " offset={offset}").unwrap();
        }
        visitor.write_str(&f);
        self.source.visit(visitor);
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

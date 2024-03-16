mod binder;

use crate::{
    catalog::{self, TableFnPtr, TableId},
    connection::QueryContext,
    parser::{self, BinaryOp, NullOrder, Order, UnaryOp},
    types::{Type, Value},
    CatalogError, KeyValueStore, StorageError,
};
use std::fmt::Write;

#[derive(Debug, thiserror::Error)]
pub enum PlannerError {
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

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

type PlannerResult<T> = std::result::Result<T, PlannerError>;

pub fn plan<T: KeyValueStore>(
    ctx: &QueryContext<'_, '_, T>,
    statement: parser::Statement,
) -> PlannerResult<TypedPlanNode<T>> {
    binder::Binder::new(ctx).bind(statement)
}

#[derive(Debug, Clone, Copy)]
pub struct ColumnIndex(pub usize);

impl std::fmt::Display for ColumnIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "#{}", self.0)
    }
}

pub struct TypedPlanNode<T: KeyValueStore> {
    node: PlanNode<T>,
    columns: Vec<Column>,
}

impl<T: KeyValueStore> TypedPlanNode<T> {
    pub fn into_node(self) -> PlanNode<T> {
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
    fn sink(node: PlanNode<T>) -> Self {
        Self {
            node,
            columns: Vec::new(),
        }
    }

    fn inherit_schema<F>(self, f: F) -> Self
    where
        F: FnOnce(PlanNode<T>) -> PlanNode<T>,
    {
        Self {
            node: f(self.node),
            columns: self.columns,
        }
    }

    fn resolve_column(&self, name: &str) -> PlannerResult<(ColumnIndex, &Column)> {
        self.columns
            .iter()
            .enumerate()
            .find(|(_, column)| column.name == name)
            .map(|(i, column)| (ColumnIndex(i), column))
            .ok_or(PlannerError::UnknownColumn(name.to_owned()))
    }
}

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub ty: Option<Type>,
}

impl From<catalog::Column> for Column {
    fn from(c: catalog::Column) -> Self {
        Self {
            name: c.name,
            ty: Some(c.ty),
        }
    }
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

pub enum PlanNode<T: KeyValueStore> {
    Explain(Box<PlanNode<T>>),
    CreateTable(CreateTable),
    DropTable(DropTable),
    Insert(Insert<T>),
    Update(Update<T>),
    Delete(Delete<T>),
    Values(Values),
    Scan(Scan<T>),
    Project(Project<T>),
    Filter(Filter<T>),
    Sort(Sort<T>),
    Limit(Limit<T>),
}

impl<T: KeyValueStore> PlanNode<T> {
    pub fn explain(&self) -> Vec<String> {
        let mut visitor = ExplainVisitor::default();
        self.visit(&mut visitor);
        visitor.rows
    }
}

impl<T: KeyValueStore> Explain for PlanNode<T> {
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
    pub columns: Vec<catalog::Column>,
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

pub struct Insert<T: KeyValueStore> {
    pub source: Box<PlanNode<T>>,
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
}

impl<T: KeyValueStore> Explain for Insert<T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Insert table={:?}", self.table);
        self.source.visit(visitor);
    }
}

pub struct Update<T: KeyValueStore> {
    pub source: Box<PlanNode<T>>,
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
}

impl<T: KeyValueStore> Explain for Update<T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Update table={:?}", self.table);
        self.source.visit(visitor);
    }
}

pub struct Delete<T: KeyValueStore> {
    pub source: Box<PlanNode<T>>,
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
}

impl<T: KeyValueStore> Explain for Delete<T> {
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

pub enum Scan<T: KeyValueStore> {
    SeqScan {
        table: TableId,
        columns: Vec<ColumnIndex>,
    },
    FunctionScan {
        source: Box<PlanNode<T>>,
        fn_ptr: TableFnPtr<T>,
    },
}

impl<T: KeyValueStore> Explain for Scan<T> {
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
            Self::FunctionScan { source, .. } => {
                visitor.write_str("FunctionScan");
                source.visit(visitor);
            }
        }
    }
}

pub struct Project<T: KeyValueStore> {
    pub source: Box<PlanNode<T>>,
    pub exprs: Vec<Expression>,
}

impl<T: KeyValueStore> Explain for Project<T> {
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

pub struct Filter<T: KeyValueStore> {
    pub source: Box<PlanNode<T>>,
    pub cond: Expression,
}

impl<T: KeyValueStore> Explain for Filter<T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Filter {}", self.cond);
        self.source.visit(visitor);
    }
}

pub struct Sort<T: KeyValueStore> {
    pub source: Box<PlanNode<T>>,
    pub order_by: Vec<OrderBy>,
}

impl<T: KeyValueStore> Explain for Sort<T> {
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

pub struct Limit<T: KeyValueStore> {
    pub source: Box<PlanNode<T>>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

impl<T: KeyValueStore> Explain for Limit<T> {
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

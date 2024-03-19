mod binder;

use crate::{
    catalog::{self, TableFnPtr},
    connection::QueryContext,
    parser::{self, BinaryOp, NullOrder, Order, UnaryOp},
    rows::ColumnIndex,
    storage::Table,
    types::{Type, Value},
    CatalogError, Storage, StorageError,
};
use std::fmt::Write;

#[derive(Debug, thiserror::Error)]
pub enum PlannerError {
    #[error("Table {0:?} already exists")]
    TableAlreadyExists(String),

    #[error("Unknown column {0:?}")]
    UnknownColumn(String),

    #[error("Ambiguous column {0:?}")]
    AmbiguousColumn(String),

    #[error("Duplicate column {0:?}")]
    DuplicateColumn(String),

    #[error("Multiple primary keys are not allowed")]
    MultiplePrimaryKeys,

    #[error("Primary key is required")]
    NoPrimaryKey,

    #[error("Type error")]
    TypeError,

    #[error("Storage error: {0}")]
    Storage(#[from] StorageError),

    #[error("Catalog error: {0}")]
    Catalog(#[from] CatalogError),
}

type PlannerResult<T> = std::result::Result<T, PlannerError>;

pub fn plan<'txn, 'db, T: Storage>(
    ctx: &'txn QueryContext<'txn, 'db, T>,
    statement: parser::Statement,
) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
    binder::Binder::new(ctx).bind(statement)
}

pub struct TypedPlanNode<'txn, 'db, T: Storage> {
    pub node: PlanNode<'txn, 'db, T>,
    pub columns: Vec<Column>,
}

impl<'txn, 'db, T: Storage> TypedPlanNode<'txn, 'db, T> {
    fn empty_source() -> Self {
        Self {
            node: PlanNode::Values(Values::one_empty_row()),
            columns: Vec::new(),
        }
    }

    /// Creates a node that does not produce any rows.
    fn sink(node: PlanNode<'txn, 'db, T>) -> Self {
        Self {
            node,
            columns: Vec::new(),
        }
    }

    fn inherit_schema<F>(self, f: F) -> Self
    where
        F: FnOnce(PlanNode<'txn, 'db, T>) -> PlanNode<'txn, 'db, T>,
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

impl Column {
    pub fn new(name: &str, ty: Type) -> Self {
        Self {
            name: name.to_owned(),
            ty: Some(ty),
        }
    }
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

pub enum PlanNode<'txn, 'db, T: Storage> {
    Explain(Box<PlanNode<'txn, 'db, T>>),
    CreateTable(CreateTable),
    DropTable(DropTable),
    Values(Values),
    Scan(Scan<'txn, 'db, T>),
    Project(Project<'txn, 'db, T>),
    Filter(Filter<'txn, 'db, T>),
    Sort(Sort<'txn, 'db, T>),
    Limit(Limit<'txn, 'db, T>),
    CrossProduct(CrossProduct<'txn, 'db, T>),
    Insert(Insert<'txn, 'db, T>),
    Update(Update<'txn, 'db, T>),
    Delete(Delete<'txn, 'db, T>),
}

impl<T: Storage> PlanNode<'_, '_, T> {
    pub fn explain(&self) -> Vec<String> {
        let mut visitor = ExplainVisitor::default();
        self.visit(&mut visitor);
        visitor.rows
    }
}

impl<T: Storage> Explain for PlanNode<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        visitor.depth += 1;
        match self {
            Self::Explain(n) => n.visit(visitor),
            Self::CreateTable(n) => n.visit(visitor),
            Self::DropTable(n) => n.visit(visitor),
            Self::Values(n) => n.visit(visitor),
            Self::Scan(n) => n.visit(visitor),
            Self::Project(n) => n.visit(visitor),
            Self::Filter(n) => n.visit(visitor),
            Self::Sort(n) => n.visit(visitor),
            Self::Limit(n) => n.visit(visitor),
            Self::CrossProduct(n) => n.visit(visitor),
            Self::Insert(n) => n.visit(visitor),
            Self::Update(n) => n.visit(visitor),
            Self::Delete(n) => n.visit(visitor),
        }
        visitor.depth -= 1;
    }
}

pub struct CreateTable {
    pub name: String,
    pub columns: Vec<catalog::Column>,
    pub constraints: Vec<catalog::Constraint>,
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
            f.push_str(if i == 0 { "(" } else { ", (" });
            for (j, value) in row.iter().enumerate() {
                if j == 0 {
                    write!(&mut f, "{value:?}").unwrap();
                } else {
                    write!(&mut f, ", {value:?}").unwrap();
                }
            }
            f.push(')');
        }
        visitor.write_str(&f);
    }
}

pub enum Scan<'txn, 'db, T: Storage> {
    SeqScan {
        table: Table<'txn, 'db, T>,
    },
    FunctionScan {
        source: Box<PlanNode<'txn, 'db, T>>,
        fn_ptr: TableFnPtr<T>,
    },
}

impl<T: Storage> Explain for Scan<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        match self {
            Self::SeqScan { table } => {
                write!(visitor, "SeqScan table={:?}", table.name());
            }
            Self::FunctionScan { source, .. } => {
                visitor.write_str("FunctionScan");
                source.visit(visitor);
            }
        }
    }
}

pub struct Project<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub exprs: Vec<Expression>,
}

impl<T: Storage> Explain for Project<'_, '_, T> {
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

pub struct Filter<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub cond: Expression,
}

impl<T: Storage> Explain for Filter<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Filter {}", self.cond);
        self.source.visit(visitor);
    }
}

pub struct Sort<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub order_by: Vec<OrderBy>,
}

impl<T: Storage> Explain for Sort<'_, '_, T> {
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

pub struct Limit<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub limit: Option<Expression>,
    pub offset: Option<Expression>,
}

impl<T: Storage> Explain for Limit<'_, '_, T> {
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

pub struct CrossProduct<'txn, 'db, T: Storage> {
    pub left: Box<PlanNode<'txn, 'db, T>>,
    pub right: Box<PlanNode<'txn, 'db, T>>,
}

impl<T: Storage> Explain for CrossProduct<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        visitor.write_str("CrossProduct");
        self.left.visit(visitor);
        self.right.visit(visitor);
    }
}

pub struct Insert<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
}

impl<T: Storage> Explain for Insert<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Insert table={:?}", self.table.name());
        self.source.visit(visitor);
    }
}

pub struct Update<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
}

impl<T: Storage> Explain for Update<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Update table={:?}", self.table.name());
        self.source.visit(visitor);
    }
}

pub struct Delete<'txn, 'db, T: Storage> {
    pub source: Box<PlanNode<'txn, 'db, T>>,
    pub table: Table<'txn, 'db, T>,
}

impl<T: Storage> Explain for Delete<'_, '_, T> {
    fn visit(&self, visitor: &mut ExplainVisitor) {
        write!(visitor, "Delete table={:?}", self.table.name());
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

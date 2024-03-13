mod binder;

use crate::{
    parser,
    storage::{TableId, Transaction},
    types::{Type, Value},
    BinaryOp, KeyValueStore, NullOrder, Order, StorageError,
};

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
            node: PlanNode::Constant(Constant::one_empty_row()),
            columns: Vec::new(),
        }
    }

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
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result;

    fn explain_child(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        let depth = depth + 1;
        for _ in 0..depth {
            f.write_str("  ")?;
        }
        self.explain(f, depth)
    }
}

pub enum PlanNode {
    Explain(Box<PlanNode>),
    CreateTable(CreateTable),
    DropTable(DropTable),
    Insert(Insert),
    Update(Update),
    Delete(Delete),
    Constant(Constant),
    Scan(Scan),
    Project(Project),
    Filter(Filter),
    Sort(Sort),
    Limit(Limit),
}

impl Explain for PlanNode {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        match self {
            Self::Explain(explain) => explain.explain(f, depth),
            Self::CreateTable(create_table) => create_table.explain(f, depth),
            Self::DropTable(drop_table) => drop_table.explain(f, depth),
            Self::Insert(insert) => insert.explain(f, depth),
            Self::Update(update) => update.explain(f, depth),
            Self::Delete(delete) => delete.explain(f, depth),
            Self::Constant(constant) => constant.explain(f, depth),
            Self::Scan(scan) => scan.explain(f, depth),
            Self::Project(project) => project.explain(f, depth),
            Self::Filter(filter) => filter.explain(f, depth),
            Self::Sort(sort) => sort.explain(f, depth),
            Self::Limit(limit) => limit.explain(f, depth),
        }
    }
}

impl std::fmt::Display for PlanNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.explain(f, 0)
    }
}

pub struct CreateTable(pub parser::CreateTable);

impl Explain for CreateTable {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, _: usize) -> std::fmt::Result {
        write!(f, "CreateTable {:?}", self.0.name)
    }
}

pub struct DropTable(pub parser::DropTable);

impl Explain for DropTable {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, _: usize) -> std::fmt::Result {
        write!(f, "DropTable {:?}", self.0.name)
    }
}

pub struct Insert {
    pub source: Box<PlanNode>,
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
}

impl Explain for Insert {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        writeln!(f, "Insert table={:?}", self.table)?;
        self.source.explain_child(f, depth)
    }
}

pub struct Update {
    pub source: Box<PlanNode>,
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
}

impl Explain for Update {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        writeln!(f, "Update table={:?}", self.table)?;
        self.source.explain_child(f, depth)
    }
}

pub struct Delete {
    pub source: Box<PlanNode>,
    pub table: TableId,
    pub primary_key_column: ColumnIndex,
}

impl Explain for Delete {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, depth: usize) -> std::fmt::Result {
        writeln!(f, "Delete table={:?}", self.table)?;
        self.source.explain_child(f, depth)
    }
}

#[derive(Debug, Clone)]
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

pub struct Constant {
    pub rows: Vec<Vec<Value>>,
}

impl Constant {
    fn new(rows: Vec<Vec<Value>>) -> Self {
        Self { rows }
    }

    fn one_empty_row() -> Self {
        Self::new(vec![Vec::new()])
    }
}

impl Explain for Constant {
    fn explain(&self, f: &mut std::fmt::Formatter<'_>, _: usize) -> std::fmt::Result {
        f.write_str("Constant ")?;
        for (i, row) in self.rows.iter().enumerate() {
            if i == 0 {
                f.write_str("[")?;
            } else {
                f.write_str(", [")?;
            }
            for (j, value) in row.iter().enumerate() {
                if j == 0 {
                    write!(f, "{value:?}")?;
                } else {
                    write!(f, ", {value:?}")?;
                }
            }
            f.write_str("]")?;
        }
        Ok(())
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

use super::{ColumnIndex, Error, PlanNode, Result, TypedPlanNode};
use crate::{
    parser, planner, storage::Transaction, types::Type, BinaryOp, KeyValueStore, StorageError,
    UnaryOp,
};
use std::collections::HashSet;

pub struct Binder<'txn, 'storage, T: KeyValueStore> {
    txn: &'txn Transaction<'storage, T>,
}

impl<'txn, 'storage, T: KeyValueStore> Binder<'txn, 'storage, T> {
    pub fn new(txn: &'txn Transaction<'storage, T>) -> Self {
        Self { txn }
    }

    pub fn bind(&self, statement: parser::Statement) -> Result<TypedPlanNode> {
        match statement {
            parser::Statement::Explain(statement) => self.bind_explain(*statement),
            parser::Statement::CreateTable(create_table) => self.bind_create_table(create_table),
            parser::Statement::DropTable(drop_table) => self.bind_drop_table(drop_table),
            parser::Statement::Insert(insert) => self.bind_insert(insert),
            parser::Statement::Values(values) => bind_values(values),
            parser::Statement::Select(select) => self.bind_select(select),
            parser::Statement::Update(update) => self.bind_update(update),
            parser::Statement::Delete(delete) => self.bind_delete(delete),
        }
    }

    fn bind_explain(&self, statement: parser::Statement) -> Result<TypedPlanNode> {
        let plan = self.bind(statement)?;
        Ok(TypedPlanNode {
            node: PlanNode::Explain(Box::new(plan.node)),
            columns: vec![planner::Column {
                name: "plan".to_owned(),
                ty: Type::Text.into(),
            }],
        })
    }

    fn bind_create_table(&self, create_table: parser::CreateTable) -> Result<TypedPlanNode> {
        let mut column_names = HashSet::new();
        for column in &create_table.columns {
            if !column_names.insert(column.name.as_str()) {
                return Err(Error::DuplicateColumn(column.name.clone()));
            }
        }
        match self.txn.table(&create_table.name) {
            Ok(_) if create_table.if_not_exists => return Ok(TypedPlanNode::empty_source()),
            Ok(_) => return Err(Error::TableAlreadyExists(create_table.name.clone())),
            Err(StorageError::UnknownTable(_)) => (),
            Err(err) => return Err(err.into()),
        };
        let create_table = planner::CreateTable {
            name: create_table.name,
            columns: create_table.columns,
        };
        Ok(TypedPlanNode::sink(PlanNode::CreateTable(create_table)))
    }

    fn bind_drop_table(&self, drop_table: parser::DropTable) -> Result<TypedPlanNode> {
        match self.txn.table(&drop_table.name) {
            Ok(_) => Ok(TypedPlanNode::sink(PlanNode::DropTable(
                planner::DropTable {
                    name: drop_table.name,
                },
            ))),
            Err(StorageError::UnknownTable(_)) if drop_table.if_exists => {
                Ok(TypedPlanNode::empty_source())
            }
            Err(err) => Err(err.into()),
        }
    }

    fn bind_insert(&self, insert: parser::Insert) -> Result<TypedPlanNode> {
        let table = self.txn.table(&insert.table_name)?;
        let TypedPlanNode { node, columns } = bind_values(insert.values)?;
        if table.columns.len() != columns.len() {
            return Err(Error::TypeError);
        }
        let mut primary_key_column = None;
        for (i, (actual, expected)) in columns.iter().zip(table.columns.iter()).enumerate() {
            if let Some(actual) = actual.ty {
                if actual != expected.ty {
                    return Err(Error::TypeError);
                }
            }
            if expected.is_primary_key {
                assert!(
                    primary_key_column.is_none(),
                    "table has multiple primary keys"
                );
                primary_key_column = Some(ColumnIndex(i));
            }
        }
        Ok(TypedPlanNode::sink(PlanNode::Insert(planner::Insert {
            source: Box::new(node),
            table: table.id,
            primary_key_column: primary_key_column.expect("table has no primary key"),
        })))
    }

    fn bind_select(&self, select: parser::Select) -> Result<TypedPlanNode> {
        let mut node = match select.from {
            Some(table_ref) => self.bind_table_ref(table_ref)?,
            None => TypedPlanNode::empty_source(),
        };
        if let Some(where_clause) = select.where_clause {
            node = bind_where_clause(node, where_clause)?;
        }
        let node = bind_order_by(node, select.order_by)?;
        let node = bind_limit(node, select.limit, select.offset)?;
        let node = bind_projections(node, select.projections)?;
        Ok(node)
    }

    fn bind_update(&self, update: parser::Update) -> Result<TypedPlanNode> {
        let table = self.txn.table(&update.table_name)?;
        let primary_key_column = table
            .columns
            .iter()
            .position(|column| column.is_primary_key)
            .map(ColumnIndex)
            .expect("table has no primary key");
        let mut node = self.bind_base_table(&update.table_name)?;
        if let Some(where_clause) = update.where_clause {
            node = bind_where_clause(node, where_clause)?;
        }
        let mut exprs = vec![None; table.columns.len()];
        for set in update.sets {
            let (index, column) = node.resolve_column(&set.column_name)?;
            let expr = &mut exprs[index.0];
            match expr {
                Some(_) => return Err(Error::DuplicateColumn(column.name.clone())),
                None => {
                    *expr = bind_expr(&node, set.expr)?.expect_type(column.ty)?.into();
                }
            }
        }
        let exprs = exprs
            .into_iter()
            .enumerate()
            .map(|(i, expr)| {
                expr.unwrap_or(planner::Expression::ColumnRef {
                    column: ColumnIndex(i),
                })
            })
            .collect();
        let node = PlanNode::Project(planner::Project {
            source: Box::new(node.node),
            exprs,
        });
        Ok(TypedPlanNode::sink(PlanNode::Update(planner::Update {
            source: Box::new(node),
            table: table.id,
            primary_key_column,
        })))
    }

    fn bind_delete(&self, delete: parser::Delete) -> Result<TypedPlanNode> {
        let table = self.txn.table(&delete.table_name)?;
        let primary_key_column = table
            .columns
            .iter()
            .position(|column| column.is_primary_key)
            .map(ColumnIndex)
            .expect("table has no primary key");
        let mut node = self.bind_base_table(&delete.table_name)?;
        if let Some(where_clause) = delete.where_clause {
            node = bind_where_clause(node, where_clause)?;
        }
        Ok(TypedPlanNode::sink(PlanNode::Delete(planner::Delete {
            source: Box::new(node.node),
            table: table.id,
            primary_key_column,
        })))
    }

    fn bind_table_ref(&self, table_ref: parser::TableRef) -> Result<TypedPlanNode> {
        match table_ref {
            parser::TableRef::BaseTable { name } => self.bind_base_table(&name),
        }
    }

    fn bind_base_table(&self, name: &str) -> Result<TypedPlanNode> {
        let table = self.txn.table(name)?;
        Ok(TypedPlanNode {
            node: PlanNode::Scan(planner::Scan::SeqScan {
                table: table.id,
                columns: (0..table.columns.len()).map(ColumnIndex).collect(),
            }),
            columns: table
                .columns
                .into_iter()
                .map(|column| planner::Column {
                    name: column.name,
                    ty: column.ty.into(),
                })
                .collect(),
        })
    }
}

fn bind_values(values: parser::Values) -> Result<TypedPlanNode> {
    let mut rows = Vec::with_capacity(values.rows.len());
    let mut columns;
    let mut rows_iter = values.rows.into_iter();
    {
        let row = rows_iter.next().unwrap();
        let mut exprs = Vec::with_capacity(row.len());
        columns = Vec::with_capacity(row.len());
        let row = row.into_iter().enumerate();
        for (i, expr) in row {
            let TypedExpression { expr, ty } = bind_expr(&TypedPlanNode::empty_source(), expr)?;
            exprs.push(expr);
            columns.push(planner::Column {
                name: format!("column{}", i + 1),
                ty,
            });
        }
        rows.push(exprs);
    }
    for row in rows_iter {
        assert_eq!(row.len(), columns.len());
        let mut exprs = Vec::with_capacity(row.len());
        for (expr, column) in row.into_iter().zip(columns.iter()) {
            let expr = bind_expr(&TypedPlanNode::empty_source(), expr)?.expect_type(column.ty)?;
            exprs.push(expr);
        }
        rows.push(exprs);
    }
    let node = PlanNode::Values(planner::Values { rows });
    Ok(TypedPlanNode { node, columns })
}

fn bind_where_clause(source: TypedPlanNode, expr: parser::Expression) -> Result<TypedPlanNode> {
    let cond = bind_expr(&source, expr)?.expect_type(Type::Boolean)?;
    Ok(source.inherit_schema(|source| {
        PlanNode::Filter(planner::Filter {
            source: Box::new(source),
            cond,
        })
    }))
}

fn bind_projections(
    source: TypedPlanNode,
    projections: Vec<parser::Projection>,
) -> Result<TypedPlanNode> {
    let mut exprs = Vec::new();
    let mut columns = Vec::new();
    for projection in projections {
        match projection {
            parser::Projection::Wildcard => {
                for (i, column) in source.columns.iter().cloned().enumerate() {
                    exprs.push(planner::Expression::ColumnRef {
                        column: ColumnIndex(i),
                    });
                    columns.push(column);
                }
            }
            parser::Projection::Expression { expr, alias } => {
                let TypedExpression { expr, ty } = bind_expr(&source, expr)?;
                let name = alias.unwrap_or_else(|| match expr {
                    planner::Expression::ColumnRef { column } => {
                        source.columns[column.0].name.clone()
                    }
                    _ => expr.to_string(),
                });
                exprs.push(expr);
                columns.push(planner::Column { name, ty });
            }
        }
    }
    Ok(TypedPlanNode {
        node: PlanNode::Project(planner::Project {
            source: Box::new(source.node),
            exprs,
        }),
        columns,
    })
}

fn bind_order_by(source: TypedPlanNode, order_by: Vec<parser::OrderBy>) -> Result<TypedPlanNode> {
    if order_by.is_empty() {
        return Ok(source);
    }
    let mut bound_order_by = Vec::with_capacity(order_by.len());
    for item in order_by {
        let TypedExpression { expr, .. } = bind_expr(&source, item.expr)?;
        bound_order_by.push(planner::OrderBy {
            expr,
            order: item.order,
            null_order: item.null_order,
        });
    }
    Ok(source.inherit_schema(|source| {
        PlanNode::Sort(planner::Sort {
            source: source.into(),
            order_by: bound_order_by,
        })
    }))
}

fn bind_limit(
    source: TypedPlanNode,
    limit: Option<parser::Expression>,
    offset: Option<parser::Expression>,
) -> Result<TypedPlanNode> {
    fn bind(
        source: &TypedPlanNode,
        expr: Option<parser::Expression>,
    ) -> Result<Option<planner::Expression>> {
        let Some(expr) = expr else {
            return Ok(None);
        };
        let expr = bind_expr(source, expr)?.expect_type(Type::Integer)?;
        Ok(Some(expr))
    }

    if limit.is_none() && offset.is_none() {
        return Ok(source);
    }

    let limit = bind(&source, limit)?;
    let offset = bind(&source, offset)?;
    Ok(source.inherit_schema(|source| {
        PlanNode::Limit(planner::Limit {
            source: Box::new(source),
            limit,
            offset,
        })
    }))
}

fn bind_expr(source: &TypedPlanNode, expr: parser::Expression) -> Result<TypedExpression> {
    match expr {
        parser::Expression::Constant(value) => {
            let ty = value.ty();
            Ok(TypedExpression {
                expr: planner::Expression::Constact(value),
                ty,
            })
        }
        parser::Expression::ColumnRef(column) => {
            let (index, column) = source.resolve_column(&column)?;
            let expr = planner::Expression::ColumnRef { column: index };
            Ok(TypedExpression {
                expr,
                ty: column.ty,
            })
        }
        parser::Expression::UnaryOp { op, expr } => {
            let TypedExpression { expr, ty } = bind_expr(source, *expr)?;
            let ty = match (op, ty) {
                (UnaryOp::Not, _) => Some(Type::Boolean),
                (UnaryOp::Plus | UnaryOp::Minus, Some(ty)) if ty.is_numeric() => Some(ty),
                (UnaryOp::Plus | UnaryOp::Minus, None) => None,
                _ => return Err(Error::TypeError),
            };
            let expr = planner::Expression::UnaryOp {
                op,
                expr: expr.into(),
            };
            Ok(TypedExpression { expr, ty })
        }
        parser::Expression::BinaryOp { op, lhs, rhs } => {
            let lhs = bind_expr(source, *lhs)?;
            let rhs = bind_expr(source, *rhs)?;
            let ty = match op {
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    match (lhs.ty, rhs.ty) {
                        (Some(Type::Integer), Some(Type::Integer)) => Some(Type::Integer),
                        (Some(ty), Some(Type::Real)) | (Some(Type::Real), Some(ty))
                            if ty.is_numeric() =>
                        {
                            Some(Type::Real)
                        }
                        (None, _) => rhs.ty,
                        (_, None) => lhs.ty,
                        _ => return Err(Error::TypeError),
                    }
                }
                BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge
                | BinaryOp::And
                | BinaryOp::Or => match (lhs.ty, rhs.ty) {
                    (Some(lhs_ty), Some(rhs_ty)) if lhs_ty != rhs_ty => {
                        return Err(Error::TypeError)
                    }
                    _ => Some(Type::Boolean),
                },
                BinaryOp::Concat => match (lhs.ty, rhs.ty) {
                    (Some(Type::Text) | None, Some(Type::Text) | None) => Some(Type::Text),
                    _ => return Err(Error::TypeError),
                },
            };
            let expr = planner::Expression::BinaryOp {
                op,
                lhs: lhs.expr.into(),
                rhs: rhs.expr.into(),
            };
            Ok(TypedExpression { expr, ty })
        }
    }
}

struct TypedExpression {
    expr: planner::Expression,
    ty: Option<Type>,
}

impl TypedExpression {
    fn expect_type<T: Into<Option<Type>>>(self, expected: T) -> Result<planner::Expression> {
        if let (Some(actual), Some(expected)) = (self.ty, expected.into()) {
            if actual != expected {
                return Err(Error::TypeError);
            }
        }
        Ok(self.expr)
    }
}

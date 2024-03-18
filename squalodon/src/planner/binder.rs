use super::{ColumnIndex, PlanNode, PlannerError, PlannerResult, TypedPlanNode, Values};
use crate::{
    catalog,
    connection::QueryContext,
    parser::{self, BinaryOp, UnaryOp},
    planner,
    storage::{Blackhole, Table},
    types::Type,
    CatalogError, Storage,
};
use std::collections::HashSet;

pub struct Binder<'txn, 'db, T: Storage> {
    ctx: &'txn QueryContext<'txn, 'db, T>,
}

impl<'txn, 'db, T: Storage> Binder<'txn, 'db, T> {
    pub fn new(ctx: &'txn QueryContext<'txn, 'db, T>) -> Self {
        Self { ctx }
    }

    pub fn bind(&self, statement: parser::Statement) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        match statement {
            parser::Statement::Explain(statement) => self.bind_explain(*statement),
            parser::Statement::Transaction(_) => unreachable!("handled before binding"),
            parser::Statement::ShowTables => {
                self.rewrite_to("SELECT * FROM squalodon_tables() ORDER BY name")
            }
            parser::Statement::Describe(name) => self.rewrite_to(&format!(
                "SELECT column_name, type, is_nullable, is_primary_key
                FROM squalodon_columns()
                WHERE table_name = {}",
                parser::quote(&name, '\'')
            )),
            parser::Statement::CreateTable(create_table) => self.bind_create_table(create_table),
            parser::Statement::DropTable(drop_table) => self.bind_drop_table(drop_table),
            parser::Statement::Values(values) => bind_values(values),
            parser::Statement::Select(select) => self.bind_select(select),
            parser::Statement::Insert(insert) => self.bind_insert(insert),
            parser::Statement::Update(update) => self.bind_update(update),
            parser::Statement::Delete(delete) => self.bind_delete(delete),
        }
    }

    fn rewrite_to(&self, sql: &str) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let mut parser = parser::Parser::new(sql);
        let statement = parser.next().unwrap().unwrap();
        assert!(parser.next().is_none());
        self.bind(statement)
    }

    fn bind_explain(
        &self,
        statement: parser::Statement,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let plan = self.bind(statement)?;
        Ok(TypedPlanNode {
            node: PlanNode::Explain(Box::new(plan.node)),
            columns: vec![planner::Column::new("plan", Type::Text)],
        })
    }

    fn bind_create_table(
        &self,
        create_table: parser::CreateTable,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let mut column_names = HashSet::new();
        for column in &create_table.columns {
            if !column_names.insert(column.name.as_str()) {
                return Err(PlannerError::DuplicateColumn(column.name.clone()));
            }
        }
        match self.ctx.catalog().table(create_table.name.clone()) {
            Ok(_) if create_table.if_not_exists => return Ok(TypedPlanNode::empty_source()),
            Ok(_) => return Err(PlannerError::TableAlreadyExists(create_table.name)),
            Err(CatalogError::UnknownEntry(_, _)) => (),
            Err(err) => return Err(err.into()),
        };
        let mut bound_constraints = HashSet::new();
        let mut has_primary_key = false;
        for constraint in create_table.constraints {
            match constraint {
                parser::Constraint::PrimaryKey(columns) => {
                    if has_primary_key {
                        return Err(PlannerError::MultiplePrimaryKeys);
                    }
                    has_primary_key = true;
                    let mut indices = Vec::with_capacity(columns.len());
                    let mut column_names = HashSet::new();
                    for column in &columns {
                        if !column_names.insert(column.as_str()) {
                            return Err(PlannerError::DuplicateColumn(column.clone()));
                        }
                        let (index, _) = create_table
                            .columns
                            .iter()
                            .enumerate()
                            .find(|(_, def)| def.name == *column)
                            .ok_or_else(|| PlannerError::UnknownColumn(column.clone()))?;
                        let index = ColumnIndex(index);
                        indices.push(index);
                        bound_constraints.insert(catalog::Constraint::NotNull(index));
                    }
                    bound_constraints.insert(catalog::Constraint::PrimaryKey(indices));
                }
                parser::Constraint::NotNull(column) => {
                    let (index, _) = create_table
                        .columns
                        .iter()
                        .enumerate()
                        .find(|(_, def)| def.name == column)
                        .ok_or_else(|| PlannerError::UnknownColumn(column.clone()))?;
                    bound_constraints.insert(catalog::Constraint::NotNull(ColumnIndex(index)));
                }
            }
        }
        if !has_primary_key {
            return Err(PlannerError::NoPrimaryKey);
        }
        let create_table = planner::CreateTable {
            name: create_table.name,
            columns: create_table.columns,
            constraints: bound_constraints.into_iter().collect(),
        };
        Ok(TypedPlanNode::sink(PlanNode::CreateTable(create_table)))
    }

    fn bind_drop_table(
        &self,
        drop_table: parser::DropTable,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        match self.ctx.catalog().table(drop_table.name.clone()) {
            Ok(_) => Ok(TypedPlanNode::sink(PlanNode::DropTable(
                planner::DropTable {
                    name: drop_table.name,
                },
            ))),
            Err(CatalogError::UnknownEntry(_, _)) if drop_table.if_exists => {
                Ok(TypedPlanNode::empty_source())
            }
            Err(err) => Err(err.into()),
        }
    }

    fn bind_select(&self, select: parser::Select) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
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

    fn bind_insert(&self, insert: parser::Insert) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let table = self.ctx.catalog().table(insert.table_name)?;
        let TypedPlanNode { node, columns } = bind_values(insert.values)?;
        if table.columns().len() != columns.len() {
            return Err(PlannerError::TypeError);
        }
        for (actual, expected) in columns.iter().zip(table.columns().iter()) {
            if let Some(actual) = actual.ty {
                if actual != expected.ty {
                    return Err(PlannerError::TypeError);
                }
            }
        }
        Ok(TypedPlanNode::sink(PlanNode::Insert(planner::Insert {
            source: Box::new(node),
            table,
        })))
    }

    fn bind_update(&self, update: parser::Update) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let table = self.ctx.catalog().table(update.table_name)?;
        let mut node = bind_base_table(table.clone());
        if let Some(where_clause) = update.where_clause {
            node = bind_where_clause(node, where_clause)?;
        }
        let mut exprs = vec![None; table.columns().len()];
        for set in update.sets {
            let (index, column) = node.resolve_column(&set.column_name)?;
            let expr = &mut exprs[index.0];
            match expr {
                Some(_) => return Err(PlannerError::DuplicateColumn(column.name.clone())),
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
            table,
        })))
    }

    fn bind_delete(&self, delete: parser::Delete) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let table = self.ctx.catalog().table(delete.table_name)?;
        let mut node = bind_base_table(table.clone());
        if let Some(where_clause) = delete.where_clause {
            node = bind_where_clause(node, where_clause)?;
        }
        Ok(TypedPlanNode::sink(PlanNode::Delete(planner::Delete {
            source: Box::new(node.node),
            table,
        })))
    }

    fn bind_table_ref(
        &self,
        table_ref: parser::TableRef,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        match table_ref {
            parser::TableRef::BaseTable { name } => {
                Ok(bind_base_table(self.ctx.catalog().table(name)?))
            }
            parser::TableRef::Function { name, args } => self.bind_table_function(name, args),
        }
    }

    fn bind_table_function(
        &self,
        name: String,
        args: Vec<parser::Expression>,
    ) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
        let catalog = self.ctx.catalog();
        let table_function = catalog.table_function(&name)?;
        let mut exprs = Vec::with_capacity(args.len());
        for (expr, column) in args.into_iter().zip(table_function.result_columns.iter()) {
            let expr = bind_expr(&TypedPlanNode::<Blackhole>::empty_source(), expr)?
                .expect_type(column.ty)?;
            exprs.push(expr);
        }
        let node = PlanNode::Project(planner::Project {
            source: Box::new(PlanNode::Values(Values::one_empty_row())),
            exprs,
        });
        Ok(TypedPlanNode {
            node: PlanNode::Scan(planner::Scan::FunctionScan {
                source: Box::new(node),
                fn_ptr: table_function.fn_ptr,
            }),
            columns: table_function
                .result_columns
                .into_iter()
                .map(Into::into)
                .collect(),
        })
    }
}

fn bind_values<'txn, 'db, T: Storage>(
    values: parser::Values,
) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
    let mut rows = Vec::with_capacity(values.rows.len());
    let mut columns;
    let mut rows_iter = values.rows.into_iter();
    {
        let row = rows_iter.next().unwrap();
        let mut exprs = Vec::with_capacity(row.len());
        columns = Vec::with_capacity(row.len());
        let row = row.into_iter().enumerate();
        for (i, expr) in row {
            let TypedExpression { expr, ty } =
                bind_expr(&TypedPlanNode::<Blackhole>::empty_source(), expr)?;
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
            let expr = bind_expr(&TypedPlanNode::<Blackhole>::empty_source(), expr)?
                .expect_type(column.ty)?;
            exprs.push(expr);
        }
        rows.push(exprs);
    }
    let node = PlanNode::Values(planner::Values { rows });
    Ok(TypedPlanNode { node, columns })
}

fn bind_base_table<'txn, 'db, T: Storage>(
    table: Table<'txn, 'db, T>,
) -> TypedPlanNode<'txn, 'db, T> {
    let columns = table.columns().iter().cloned().map(Into::into).collect();
    TypedPlanNode {
        node: PlanNode::Scan(planner::Scan::SeqScan { table }),
        columns,
    }
}

fn bind_where_clause<'txn, 'db, T: Storage>(
    source: TypedPlanNode<'txn, 'db, T>,
    expr: parser::Expression,
) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
    let cond = bind_expr(&source, expr)?.expect_type(Type::Boolean)?;
    Ok(source.inherit_schema(|source| {
        PlanNode::Filter(planner::Filter {
            source: Box::new(source),
            cond,
        })
    }))
}

fn bind_projections<'txn, 'db, T: Storage>(
    source: TypedPlanNode<'txn, 'db, T>,
    projections: Vec<parser::Projection>,
) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
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

fn bind_order_by<'txn, 'db, T: Storage>(
    source: TypedPlanNode<'txn, 'db, T>,
    order_by: Vec<parser::OrderBy>,
) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
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

fn bind_limit<'txn, 'db, T: Storage>(
    source: TypedPlanNode<'txn, 'db, T>,
    limit: Option<parser::Expression>,
    offset: Option<parser::Expression>,
) -> PlannerResult<TypedPlanNode<'txn, 'db, T>> {
    fn bind<T: Storage>(
        source: &TypedPlanNode<T>,
        expr: Option<parser::Expression>,
    ) -> PlannerResult<Option<planner::Expression>> {
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

fn bind_expr<T: Storage>(
    source: &TypedPlanNode<T>,
    expr: parser::Expression,
) -> PlannerResult<TypedExpression> {
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
                _ => return Err(PlannerError::TypeError),
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
                        _ => return Err(PlannerError::TypeError),
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
                        return Err(PlannerError::TypeError)
                    }
                    _ => Some(Type::Boolean),
                },
                BinaryOp::Concat => match (lhs.ty, rhs.ty) {
                    (Some(Type::Text) | None, Some(Type::Text) | None) => Some(Type::Text),
                    _ => return Err(PlannerError::TypeError),
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
    fn expect_type<T: Into<Option<Type>>>(self, expected: T) -> PlannerResult<planner::Expression> {
        if let (Some(actual), Some(expected)) = (self.ty, expected.into()) {
            if actual != expected {
                return Err(PlannerError::TypeError);
            }
        }
        Ok(self.expr)
    }
}

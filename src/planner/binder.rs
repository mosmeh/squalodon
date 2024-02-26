use super::{ColumnIndex, Error, PlanKind, PlanNode, Result};
use crate::{
    parser, planner,
    storage::Catalog,
    types::{Type, Value},
    BinaryOp,
};

pub struct Binder<'a> {
    catalog: &'a Catalog,
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self { catalog }
    }

    pub fn bind(&mut self, statement: parser::Statement) -> Result<PlanNode> {
        match statement {
            parser::Statement::Explain(statement) => self.bind_explain(*statement),
            parser::Statement::CreateTable(create_table) => Ok(bind_create_table(create_table)),
            parser::Statement::Insert(insert) => self.bind_insert(insert),
            parser::Statement::Select(select) => self.bind_select(select),
        }
    }

    fn bind_explain(&mut self, statement: parser::Statement) -> Result<PlanNode> {
        let plan = self.bind(statement)?;
        Ok(PlanNode {
            kind: PlanKind::Explain(Box::new(plan)),
            schema: planner::Schema::empty().into(),
        })
    }

    fn bind_insert(&self, insert: parser::Insert) -> Result<PlanNode> {
        let table_id = self.catalog.table_id(&insert.table_name)?;
        let mut exprs = Vec::with_capacity(insert.column_names.len());
        let table = self.catalog.table(table_id)?;
        let column_names: Vec<_> = if insert.column_names.is_empty() {
            table.columns.iter().map(|column| &column.name).collect()
        } else {
            insert.column_names.iter().collect()
        };
        let mut primary_key_column = None;
        for column in &table.columns {
            let index_in_values = column_names.iter().position(|name| *name == &column.name);
            let expr = if let Some(index) = index_in_values {
                let TypedExpression { expr, ty } =
                    bind_expr(&PlanNode::empty_row(), insert.exprs[index].clone())?;
                if let Some(ty) = ty {
                    if ty != column.ty {
                        return Err(Error::TypeError);
                    }
                }
                expr
            } else {
                if column.is_not_null {
                    return Err(Error::TypeError);
                }
                planner::Expression::Constact(Value::Null)
            };
            if column.is_primary_key {
                assert!(
                    primary_key_column.is_none(),
                    "composite primary key is not supported"
                );
                primary_key_column = Some(ColumnIndex(exprs.len()));
            }
            exprs.push(expr);
        }
        Ok(PlanNode {
            kind: PlanKind::Insert(planner::Insert {
                table: table_id,
                primary_key_column: primary_key_column.expect("table has no primary key"),
                exprs,
            }),
            schema: planner::Schema::empty().into(),
        })
    }

    fn bind_select(&mut self, select: parser::Select) -> Result<PlanNode> {
        let mut node = match select.from {
            Some(table_ref) => self.bind_table_ref(table_ref)?,
            None => PlanNode::empty_row(),
        };
        if let Some(where_clause) = select.where_clause {
            node = bind_where_clause(node, where_clause)?;
        }
        node = bind_projections(node, select.projections)?;
        assert!(select.order_by.is_empty(), "ORDER BY is not supported");
        assert!(select.limit.is_none(), "LIMIT is not supported");
        Ok(node)
    }

    fn bind_table_ref(&self, table_ref: parser::TableRef) -> Result<PlanNode> {
        match table_ref {
            parser::TableRef::BaseTable { name } => self.bind_base_table(&name),
        }
    }

    fn bind_base_table(&self, name: &str) -> Result<PlanNode> {
        let table_id = self.catalog.table_id(name)?;
        let table = self.catalog.table(table_id)?;
        Ok(PlanNode {
            kind: PlanKind::Scan(planner::Scan::SeqScan {
                table: table_id,
                columns: (0..table.columns.len()).map(ColumnIndex).collect(),
            }),
            schema: planner::Schema::new(
                table
                    .columns
                    .into_iter()
                    .map(|column| planner::Column {
                        name: column.name,
                        ty: column.ty.into(),
                    })
                    .collect(),
            )
            .into(),
        })
    }
}

fn bind_create_table(create_table: parser::CreateTable) -> PlanNode {
    PlanNode {
        kind: PlanKind::CreateTable(planner::CreateTable(create_table)),
        schema: planner::Schema::empty().into(),
    }
}

fn bind_where_clause(source: PlanNode, expr: parser::Expression) -> Result<PlanNode> {
    let cond = bind_expr(&source, expr)?.expr;
    let schema = source.schema.clone();
    Ok(PlanNode {
        kind: PlanKind::Filter(planner::Filter {
            source: Box::new(source),
            cond,
        }),
        schema,
    })
}

fn bind_projections(source: PlanNode, projections: Vec<parser::Projection>) -> Result<PlanNode> {
    let mut exprs = Vec::new();
    let mut columns = Vec::new();
    for projection in projections {
        match projection {
            parser::Projection::Wildcard => {
                for (i, column) in source.schema.columns.iter().cloned().enumerate() {
                    exprs.push(planner::Expression::ColumnRef {
                        column: ColumnIndex(i),
                    });
                    columns.push(column);
                }
            }
            parser::Projection::Expression { expr, alias } => {
                let TypedExpression { expr, ty } = bind_expr(&source, expr)?;
                let name = alias.unwrap_or_else(|| expr.to_string());
                exprs.push(expr);
                columns.push(planner::Column { name, ty });
            }
        }
    }
    Ok(PlanNode {
        kind: PlanKind::Project(planner::Project {
            source: Box::new(source),
            exprs,
        }),
        schema: planner::Schema::new(columns).into(),
    })
}

fn bind_expr(source: &PlanNode, expr: parser::Expression) -> Result<TypedExpression> {
    match expr {
        parser::Expression::Constant(value) => {
            let ty = value.ty();
            Ok(TypedExpression {
                expr: planner::Expression::Constact(value),
                ty,
            })
        }
        parser::Expression::ColumnRef(column) => {
            let (index, column) = source.schema.resolve_column(&column)?;
            let expr = planner::Expression::ColumnRef { column: index };
            Ok(TypedExpression {
                expr,
                ty: column.ty,
            })
        }
        parser::Expression::BinaryOp { op, lhs, rhs } => {
            let lhs = bind_expr(source, *lhs)?;
            let rhs = bind_expr(source, *rhs)?;
            match (lhs.ty, rhs.ty) {
                (Some(lhs_ty), Some(rhs_ty)) if lhs_ty != rhs_ty => return Err(Error::TypeError),
                _ => (),
            }
            let ty = match op {
                BinaryOp::Add | BinaryOp::Sub | BinaryOp::Mul | BinaryOp::Div | BinaryOp::Mod => {
                    lhs.ty.or(rhs.ty)
                }
                BinaryOp::Eq
                | BinaryOp::Ne
                | BinaryOp::Lt
                | BinaryOp::Le
                | BinaryOp::Gt
                | BinaryOp::Ge
                | BinaryOp::And
                | BinaryOp::Or => Some(Type::Boolean),
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

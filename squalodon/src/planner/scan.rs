use super::{
    explain::ExplainFormatter,
    expression::{ExpressionBinder, PlanExpression},
    Column, ColumnId, ColumnMap, Filter, Node, PlanNode, Planner, PlannerResult,
};
use crate::{
    catalog::{Index, Table, TableFunction},
    parser,
    planner::expression::TypedExpression,
    types::NullableType,
    PlannerError, Value,
};
use std::ops::Bound;

#[derive(Clone)]
pub enum Scan<'a> {
    Seq {
        table: Table<'a>,
        outputs: Vec<ColumnId>,
    },
    Index {
        index: Index<'a>,
        range: (Bound<Vec<Value>>, Bound<Vec<Value>>),
        outputs: Vec<ColumnId>,
    },
    IndexOnly {
        index: Index<'a>,
        range: (Bound<Vec<Value>>, Bound<Vec<Value>>),
        outputs: Vec<ColumnId>,
    },
    Function {
        source: Box<PlanNode<'a>>,
        function: &'a TableFunction,
        outputs: Vec<ColumnId>,
    },
    Expression {
        rows: Vec<Vec<PlanExpression<'a>>>,
        outputs: Vec<ColumnId>,
    },
}

impl Node for Scan<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        match self {
            Self::Seq { table, .. } => {
                f.node("SeqScan").field("table", table.name());
            }
            Self::Index { index, range, .. } => {
                f.node("IndexScan")
                    .field("index", index.name())
                    .field("range", format_range(range));
            }
            Self::IndexOnly { index, range, .. } => {
                f.node("IndexOnlyScan")
                    .field("index", index.name())
                    .field("range", format_range(range));
            }
            Self::Function {
                source, function, ..
            } => {
                f.node("FunctionScan")
                    .field("function", function.name)
                    .child(source);
            }
            Self::Expression { rows, outputs } => {
                f.node("ExpressionScan")
                    .field("columns", outputs.len())
                    .field("rows", rows.len());
            }
        }
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        match self {
            Self::Seq { outputs, .. }
            | Self::Index { outputs, .. }
            | Self::IndexOnly { outputs, .. }
            | Self::Function { outputs, .. }
            | Self::Expression { outputs, .. } => {
                columns.extend(outputs.iter());
            }
        }
    }

    fn num_rows(&self) -> usize {
        match self {
            Self::Seq { table, .. } => table.statistics().num_rows() as usize,
            Self::Index { index, .. } | Self::IndexOnly { index, .. } => {
                let num_rows = index.table().statistics().num_rows() as f64;
                (num_rows * Filter::DEFAULT_SELECTIVITY) as usize
            }
            Self::Function { source, .. } => {
                // Assume each invocation produces one row
                source.num_rows()
            }
            Self::Expression { rows, .. } => rows.len(),
        }
    }

    fn cost(&self) -> f64 {
        match self {
            Self::Seq { .. } => self.num_rows() as f64 * PlanNode::DEFAULT_ROW_COST,
            Self::Index { .. } | Self::IndexOnly { .. } => {
                let num_rows = self.num_rows() as f64;
                // Assume the index uses a data structure where time complexity for lookup is O(log n)
                let index_lookup_cost = num_rows.log2().max(0.0) * PlanNode::DEFAULT_ROW_COST;
                let row_fetch_cost = num_rows * PlanNode::DEFAULT_ROW_COST;
                index_lookup_cost + row_fetch_cost
            }
            Self::Function { source, .. } => {
                let function_cost = source.num_rows() as f64 * PlanNode::DEFAULT_ROW_COST;
                source.cost() + function_cost
            }
            Self::Expression { rows, .. } => rows.len() as f64 * PlanNode::DEFAULT_ROW_COST,
        }
    }
}

fn format_range(range: &(Bound<Vec<Value>>, Bound<Vec<Value>>)) -> String {
    let mut s = String::new();
    match &range.0 {
        Bound::Included(values) => {
            s.push('[');
            s.push_str(&format_values(values));
        }
        Bound::Excluded(values) => {
            s.push('(');
            s.push_str(&format_values(values));
        }
        Bound::Unbounded => s.push_str("[start"),
    }
    s.push_str(" - ");
    match &range.1 {
        Bound::Included(values) => {
            s.push_str(&format_values(values));
            s.push(']');
        }
        Bound::Excluded(values) => {
            s.push_str(&format_values(values));
            s.push(')');
        }
        Bound::Unbounded => s.push_str("end]"),
    }
    s
}

fn format_values(values: &[Value]) -> String {
    values
        .iter()
        .map(Value::to_string)
        .collect::<Vec<_>>()
        .join(", ")
}

impl<'a> PlanNode<'a> {
    pub(super) fn new_table_scan(column_map: &mut ColumnMap, table: Table<'a>) -> Self {
        let outputs = table
            .columns()
            .iter()
            .map(|column| {
                column_map.insert(Column::with_table_name(
                    table.name(),
                    &column.name,
                    column.ty,
                ))
            })
            .collect();
        Self::Scan(Scan::Seq { table, outputs })
    }

    pub(super) fn new_expression_scan(
        column_map: &mut ColumnMap,
        rows: Vec<Vec<PlanExpression<'a>>>,
        column_types: Vec<NullableType>,
    ) -> Self {
        let outputs = column_types
            .into_iter()
            .enumerate()
            .map(|(i, ty)| column_map.insert(Column::new(format!("column{}", i + 1), ty)))
            .collect();
        Self::Scan(Scan::Expression { rows, outputs })
    }

    pub(super) fn new_empty_row() -> Self {
        Self::Scan(Scan::Expression {
            rows: vec![Vec::new()],
            outputs: Vec::new(),
        })
    }

    pub(super) fn new_no_rows(outputs: Vec<ColumnId>) -> Self {
        Self::Scan(Scan::Expression {
            rows: Vec::new(),
            outputs,
        })
    }

    pub(super) fn function_scan(
        self,
        column_map: &mut ColumnMap,
        function: &'a TableFunction,
    ) -> Self {
        if self.produces_no_rows() {
            return self;
        }
        let outputs = function
            .result_columns
            .iter()
            .map(|column| column_map.insert(column.clone()))
            .collect();
        Self::Scan(Scan::Function {
            source: Box::new(self),
            function,
            outputs,
        })
    }

    pub(super) fn into_no_rows(self) -> Self {
        Self::Scan(Scan::Expression {
            rows: Vec::new(),
            outputs: self.outputs(),
        })
    }
}

impl<'a> Planner<'a> {
    pub fn plan_base_table(&self, table: Table<'a>) -> PlanNode<'a> {
        PlanNode::new_table_scan(&mut self.column_map_mut(), table)
    }

    pub fn plan_table_function(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        name: String,
        args: Vec<parser::Expression>,
    ) -> PlannerResult<PlanNode<'a>> {
        let mut arg_types = Vec::with_capacity(args.len());
        let mut arg_exprs = Vec::with_capacity(args.len());
        for arg in args {
            let expr = expr_binder.bind_without_source(arg)?;
            arg_types.push(expr.ty);
            arg_exprs.push(expr);
        }
        let function = self.catalog.table_function(&name, &arg_types)?;
        let mut column_map = self.column_map_mut();
        Ok(PlanNode::new_empty_row()
            .project(&mut column_map, arg_exprs)
            .function_scan(&mut column_map, function))
    }

    pub fn plan_values(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        values: parser::Values,
    ) -> PlannerResult<PlanNode<'a>> {
        if values.rows.is_empty() {
            return Ok(PlanNode::new_empty_row());
        }

        let mut rows = Vec::with_capacity(values.rows.len());
        let num_columns = values.rows[0].len();
        let mut column_types = vec![NullableType::Null; num_columns];
        for row in values.rows {
            assert_eq!(row.len(), num_columns);
            let mut exprs = Vec::with_capacity(num_columns);
            for (expr, column_type) in row.into_iter().zip(column_types.iter_mut()) {
                let TypedExpression { expr, ty } = expr_binder.bind_without_source(expr)?;
                if !ty.is_compatible_with(*column_type) {
                    return Err(PlannerError::TypeError);
                }
                if column_type.is_null() {
                    *column_type = ty;
                }
                exprs.push(expr);
            }
            rows.push(exprs);
        }

        Ok(PlanNode::new_expression_scan(
            &mut self.column_map_mut(),
            rows,
            column_types,
        ))
    }
}

use super::{
    column::ColumnMap, explain::ExplainFormatter, expression::ExpressionBinder, Column, ColumnId,
    Node, PlanNode, Planner, PlannerResult,
};
use crate::{
    catalog::{Index, Table, TableFunction},
    parser, Value,
};
use std::ops::Bound;

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
        }
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        match self {
            Self::Seq { outputs, .. }
            | Self::Index { outputs, .. }
            | Self::IndexOnly { outputs, .. }
            | Self::Function { outputs, .. } => {
                columns.extend(outputs.iter());
            }
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
                column_map.insert(Column {
                    table_name: Some(table.name().to_owned()),
                    column_name: column.name.clone(),
                    ty: column.ty.into(),
                })
            })
            .collect();
        Self::Scan(Scan::Seq { table, outputs })
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
}

impl<'a> Planner<'a> {
    pub fn plan_base_table(&self, table: Table<'a>) -> PlanNode<'a> {
        PlanNode::new_table_scan(&mut self.column_map(), table)
    }

    pub fn plan_table_function(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        name: String,
        args: Vec<parser::Expression>,
    ) -> PlannerResult<PlanNode<'a>> {
        let function = self.ctx.catalog().table_function(&name)?;
        let exprs = args
            .into_iter()
            .map(|expr| expr_binder.bind_without_source(expr))
            .collect::<PlannerResult<_>>()?;
        let mut column_map = self.column_map();
        Ok(PlanNode::new_empty_row()
            .project(&mut column_map, exprs)
            .function_scan(&mut column_map, function))
    }
}

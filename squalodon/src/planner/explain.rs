use super::{column::ColumnMap, Column, ColumnId, Node, PlanNode, Planner, PlannerResult};
use crate::{parser, Type};
use std::{
    cell::{Ref, RefCell},
    rc::Rc,
};

#[derive(Clone)]
pub struct Explain<'a> {
    pub source: Box<PlanNode<'a>>,
    pub output: ColumnId,
    pub column_map: Rc<RefCell<ColumnMap>>,
}

impl Node for Explain<'_> {
    fn fmt_explain(&self, f: &ExplainFormatter) {
        f.node("Explain").child(&self.source);
    }

    fn append_outputs(&self, columns: &mut Vec<ColumnId>) {
        columns.push(self.output);
    }

    fn num_rows(&self) -> usize {
        1 // Lower bound
    }

    fn cost(&self) -> f64 {
        PlanNode::DEFAULT_COST
    }
}

impl Explain<'_> {
    pub fn explain(&self) -> Vec<String> {
        let f = ExplainFormatter::new(self.column_map.clone());
        self.source.fmt_explain(&f);
        f.finish()
    }
}

impl<'a> Planner<'a> {
    pub fn plan_explain(&self, statement: parser::Statement) -> PlannerResult<PlanNode<'a>> {
        Ok(self.plan(statement)?.explain(self))
    }
}

impl<'a> PlanNode<'a> {
    pub(super) fn explain(self, planner: &Planner<'a>) -> Self {
        Self::Explain(Explain {
            source: Box::new(self),
            output: planner
                .column_map_mut()
                .insert(Column::new("plan", Type::Text)),
            column_map: planner.column_map.clone(),
        })
    }
}

pub struct ExplainFormatter {
    column_map: Rc<RefCell<ColumnMap>>,
    state: RefCell<FormatterState>,
}

impl ExplainFormatter {
    pub fn new(column_map: Rc<RefCell<ColumnMap>>) -> Self {
        Self {
            column_map,
            state: Default::default(),
        }
    }

    fn finish(self) -> Vec<String> {
        self.state.into_inner().rows
    }

    pub fn node(&self, name: &str) -> ExplainNode {
        ExplainNode {
            f: self,
            name: name.to_string(),
            fields: Vec::new(),
            max_field_name_len: 0,
            children: Vec::new(),
        }
    }

    pub fn column_map(&self) -> Ref<ColumnMap> {
        self.column_map.borrow()
    }
}

#[derive(Default)]
struct FormatterState {
    rows: Vec<String>,
    prefix: String,
}

pub struct ExplainNode<'a, 'b> {
    f: &'a ExplainFormatter,
    name: String,
    fields: Vec<(String, String)>,
    max_field_name_len: usize,
    children: Vec<&'b PlanNode<'b>>,
}

impl<'a, 'b> ExplainNode<'a, 'b> {
    pub fn field(&mut self, name: &str, value: impl std::fmt::Display) -> &mut Self {
        self.max_field_name_len = self.max_field_name_len.max(name.len());
        self.fields.push((name.to_string(), value.to_string()));
        self
    }

    pub fn child(&mut self, plan: &'b PlanNode<'b>) -> &mut Self {
        self.children.push(plan);
        self
    }
}

impl Drop for ExplainNode<'_, '_> {
    fn drop(&mut self) {
        let mut f = self.f.state.borrow_mut();
        let initial_prefix_len = f.prefix.len();
        match f.prefix.pop() {
            Some(ch) => {
                assert!(ch == '|' || ch == ' ');
                let row = format!("{}+- {}", f.prefix, self.name);
                f.rows.push(row);
                f.prefix.push(ch);
                f.prefix.push_str("  "); // Padding between two '|'s
            }
            None => {
                // Top level node
                f.rows.push(self.name.clone());
            }
        }
        let mut prev_name = None;
        for (name, value) in &self.fields {
            let mut s = f.prefix.clone();
            s.push_str(if self.children.is_empty() {
                "    "
            } else {
                "|   "
            });
            if prev_name == Some(name) {
                for _ in 0..self.max_field_name_len + 2 {
                    s.push(' ');
                }
                s.push_str(value);
            } else {
                s.push_str(&format!(
                    "{name:>width$}: {value}",
                    name = name,
                    width = self.max_field_name_len
                ));
            }
            prev_name = Some(name);
            f.rows.push(s);
        }
        drop(f);
        for (i, child) in self.children.iter().enumerate() {
            self.f
                .state
                .borrow_mut()
                .prefix
                .push(if i < self.children.len() - 1 {
                    '|'
                } else {
                    ' '
                });
            child.fmt_explain(self.f);
            self.f.state.borrow_mut().prefix.pop().unwrap();
        }
        self.f
            .state
            .borrow_mut()
            .prefix
            .truncate(initial_prefix_len);
    }
}

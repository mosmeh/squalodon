use super::{
    aggregate::{AggregateCollection, AggregatePlanner},
    expression::{ExpressionBinder, TypedExpression},
    PlanNode, Planner, PlannerResult,
};
use crate::parser;
use std::collections::HashMap;

impl<'a> Planner<'a> {
    pub fn plan_query(&self, query: parser::Query) -> PlannerResult<PlanNode<'a>> {
        match query.body {
            parser::QueryBody::Select(select) => self.plan_select(select, query.modifier),
            parser::QueryBody::Union { all, left, right } => {
                self.plan_union(all, *left, *right, query.modifier)
            }
        }
    }

    fn plan_select(
        &self,
        select: parser::Select,
        modifier: parser::QueryModifier,
    ) -> PlannerResult<PlanNode<'a>> {
        let mut plan = self.plan_table_ref(&ExpressionBinder::new(self), select.from)?;

        // Expand * in SELECT clause.
        // This must be done early, because * should expand to all columns from
        // the TableRef, not the columns available just before processing SELECT.

        // Expanded expressions
        let mut projection_exprs = Vec::new();
        // Aliases for the projected columns (None for columns without aliases)
        let mut column_aliases = Vec::new();

        let outputs = plan.outputs();
        for projection in &select.projections {
            match projection {
                parser::Projection::Wildcard => {
                    let column_map = self.column_map();
                    for output in &outputs {
                        let column = &column_map[output];
                        projection_exprs
                            .push(parser::Expression::ColumnRef(column.column_ref().clone()));
                        column_aliases.push(None);
                    }
                }
                parser::Projection::Expression { expr, alias: None } => {
                    projection_exprs.push(expr.clone());
                    column_aliases.push(None);
                }
                parser::Projection::Expression {
                    alias: Some(alias), ..
                } => {
                    projection_exprs.push(parser::Expression::ColumnRef(
                        parser::ColumnRef::unqualified(alias.clone()),
                    ));
                    column_aliases.push(Some(alias.clone()));
                }
            }
        }

        if let Some(where_clause) = select.where_clause {
            // WHERE cannot refer to aliases.
            plan = self.plan_filter(&ExpressionBinder::new(self), plan, where_clause)?;
        }

        // Any occurrences of aggregate functions in SELECT, HAVING or ORDER BY
        // clauses make the query an aggregate query.
        // So we first gather all occurrences of aggregate functions into
        // aggregate_collection to determine if the query is an aggregate query.
        let mut aggregate_collection = AggregateCollection::new(self);
        for projection in &select.projections {
            match projection {
                parser::Projection::Wildcard => (),
                parser::Projection::Expression { expr, .. } => {
                    plan = aggregate_collection.gather(plan, expr)?;
                }
            }
        }
        if let Some(parser::Distinct { on: Some(on) }) = &select.distinct {
            for expr in on {
                plan = aggregate_collection.gather(plan, expr)?;
            }
        }
        if let Some(having) = &select.having {
            plan = aggregate_collection.gather(plan, having)?;
        }
        for order_by in &modifier.order_by {
            plan = aggregate_collection.gather(plan, &order_by.expr)?;
        }
        let mut aggregate_planner = aggregate_collection.finish();

        // Bind aliases in SELECT clause.
        let (mut plan, mut alias_map) =
            self.bind_column_aliases(plan, &aggregate_planner, &select.projections)?;

        // The query is an aggregate query if there are any aggregate functions
        // or if there is a GROUP BY clause.
        if aggregate_planner.has_aggregates() || !select.group_by.is_empty() {
            // GROUP BY can refer to aliases.
            let expr_binder = ExpressionBinder::new(self).with_aliases(&alias_map);
            plan = aggregate_planner.plan(&expr_binder, plan, select.group_by)?;

            // The aggregation removes non-aggregated columns and columns not
            // present in the GROUP BY clause, so we need to rebind the aliases.
            (plan, alias_map) =
                self.bind_column_aliases(plan, &aggregate_planner, &select.projections)?;
        }

        // SELECT, HAVING and ORDER BY can refer to both aliases and
        // aggregate results.
        let expr_binder = ExpressionBinder::new(self)
            .with_aliases(&alias_map)
            .with_aggregates(&aggregate_planner);

        if let Some(having) = select.having {
            plan = self.plan_filter(&expr_binder, plan, having)?;
        }

        if select.distinct.is_some() {
            // SELECT DISTINCT will not preserve the order of rows,
            // so we need to apply ORDER BY after SELECT DISTINCT.
            plan = self.plan_projections(&expr_binder, plan, projection_exprs, select.distinct)?;
            plan = self.plan_order_by(&expr_binder, plan, modifier.order_by)?;
        } else {
            // This is SELECT without DISTINCT, so we can apply ORDER BY before
            // projection to allow the ORDER BY clause to reference columns
            // not present in the SELECT clause.
            plan = self.plan_order_by(&expr_binder, plan, modifier.order_by)?;
            plan = self.plan_projections(&expr_binder, plan, projection_exprs, select.distinct)?;
        }

        // LIMIT and OFFSET cannot refer to aliases or aggregate results.
        let expr_binder = ExpressionBinder::new(self);
        let plan = self.plan_limit(&expr_binder, plan, modifier.limit, modifier.offset)?;

        // Rename the columns according to the aliases.
        let mut column_map = self.column_map_mut();
        for (id, alias) in plan.outputs().into_iter().zip(column_aliases) {
            let column = &mut column_map[id];
            column.set_table_alias(None); // Table names are not exposed outside the subquery.
            if let Some(alias) = alias {
                column.set_column_alias(alias);
            }
        }

        Ok(plan)
    }

    fn bind_column_aliases(
        &self,
        mut plan: PlanNode<'a>,
        aggregates: &AggregatePlanner<'_, 'a>,
        projections: &[parser::Projection],
    ) -> PlannerResult<(PlanNode<'a>, HashMap<String, TypedExpression<'a>>)> {
        let mut alias_map = HashMap::new();
        for projection in projections {
            if let parser::Projection::Expression {
                expr,
                alias: Some(alias),
            } = projection
            {
                let expr_binder = ExpressionBinder::new(self)
                    .with_aliases(&alias_map)
                    .with_aggregates(aggregates);
                let (new_plan, expr) = expr_binder.bind(plan, expr.clone())?;
                plan = new_plan;

                // Adding aliases one by one makes sure that aliases that
                // appear later in the SELECT clause can only refer to
                // aliases that appear earlier, preventing
                // circular references.
                // Aliases with the same name shadow previous aliases.
                alias_map.insert(alias.clone(), expr);
            }
        }
        Ok((plan, alias_map))
    }

    pub fn plan_table_ref(
        &self,
        expr_binder: &ExpressionBinder<'_, 'a>,
        table_ref: parser::TableRef,
    ) -> PlannerResult<PlanNode<'a>> {
        let plan = match table_ref.kind {
            parser::TableRefKind::BaseTable { name } => {
                self.plan_base_table(self.catalog.table(&name)?)
            }
            parser::TableRefKind::Join(join) => self.plan_join(expr_binder, *join)?,
            parser::TableRefKind::Subquery(query) => self.plan_query(*query)?,
            parser::TableRefKind::Function { name, args } => {
                self.plan_table_function(expr_binder, name, args)?
            }
            parser::TableRefKind::Values(values) => self.plan_values(expr_binder, values)?,
        };
        if let Some(alias) = table_ref.alias {
            let mut column_map = self.column_map_mut();
            for id in plan.outputs() {
                column_map[id].set_table_alias(alias.clone());
            }
        }
        Ok(plan)
    }
}

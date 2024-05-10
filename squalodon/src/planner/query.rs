use super::{
    aggregate::AggregateCollection, expression::ExpressionBinder, PlanNode, Planner, PlannerResult,
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

        // Any occurrences of aggregate functions in SELECT, HAVING and ORDER BY
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

        // Next, we expand * and resolve aliases in SELECT clause.

        // Expanded expressions
        let mut projection_exprs = Vec::new();

        // Aliases for the projected columns (None for columns without aliases)
        let mut column_aliases = Vec::new();

        // Map from alias to expression
        let mut alias_map = HashMap::new();

        let outputs = plan.outputs();
        for projection in select.projections {
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
                parser::Projection::Expression { expr, alias } => {
                    let Some(alias) = alias else {
                        projection_exprs.push(expr);
                        column_aliases.push(None);
                        continue;
                    };

                    let expr_binder = ExpressionBinder::new(self)
                        .with_aliases(&alias_map)
                        .with_aggregates(&aggregate_planner);
                    let (new_plan, expr) = expr_binder.bind(plan, expr)?;
                    plan = new_plan;

                    projection_exprs.push(parser::Expression::ColumnRef(
                        parser::ColumnRef::unqualified(alias.clone()),
                    ));
                    column_aliases.push(Some(alias.clone()));

                    // Adding aliases one by one makes sure that aliases that
                    // appear later in the SELECT clause can only refer to
                    // aliases that appear earlier, preventing
                    // circular references.
                    // Aliases with the same name shadow previous aliases.
                    alias_map.insert(alias, expr);
                }
            }
        }

        // Now we are ready to process the rest of the clauses with
        // the aliases and aggregate results.

        // WHERE and GROUP BY can refer to aliases but not aggregate results.
        let expr_binder = ExpressionBinder::new(self).with_aliases(&alias_map);

        if let Some(where_clause) = select.where_clause {
            plan = self.plan_filter(&expr_binder, plan, where_clause)?;
        }

        // The query is an aggregate query if there are any aggregate functions
        // or if there is a GROUP BY clause.
        if aggregate_planner.has_aggregates() || !select.group_by.is_empty() {
            plan = aggregate_planner.plan(&expr_binder, plan, select.group_by)?;
        }

        // SELECT, HAVING and ORDER BY can refer to both aliases and
        // aggregate results.
        let expr_binder = ExpressionBinder::new(self)
            .with_aliases(&alias_map)
            .with_aggregates(&aggregate_planner);

        if let Some(having) = select.having {
            plan = self.plan_filter(&expr_binder, plan, having)?;
        }

        // FIXME: order_by should be after projections according to SQL
        //        semantics. However, if projections reorder columns,
        //        we lose track of aggregation results. So we process order_by
        //        before projections for now.
        let plan = self.plan_order_by(&expr_binder, plan, modifier.order_by)?;
        let plan = self.plan_projections(&expr_binder, plan, projection_exprs, select.distinct)?;

        let plan = self.plan_limit(
            &ExpressionBinder::new(self),
            plan,
            modifier.limit,
            modifier.offset,
        )?;

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

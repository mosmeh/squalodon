use super::{ExecutorNode, ExecutorResult};
use crate::{
    connection::ConnectionContext,
    parser::ObjectKind,
    planner::{CreateIndex, CreateTable, DropObject, Reindex, Truncate},
    CatalogError,
};

impl ExecutorNode<'_> {
    pub fn create_table(ctx: &ConnectionContext, plan: CreateTable) -> ExecutorResult<Self> {
        let CreateTable {
            name,
            if_not_exists,
            columns,
            primary_keys,
            constraints,
            create_indexes,
        } = plan;
        let result = ctx
            .catalog()
            .create_table(&name, &columns, &primary_keys, &constraints);
        match result {
            Ok(_) => (),
            Err(CatalogError::DuplicateEntry(_, _)) if if_not_exists => (),
            Err(e) => return Err(e.into()),
        }
        for create_index in create_indexes {
            ctx.catalog().create_index(
                create_index.name,
                create_index.table_name,
                &create_index.column_indexes,
                create_index.is_unique,
            )?;
        }
        Ok(Self::empty_result())
    }

    pub fn create_index(ctx: &ConnectionContext, plan: CreateIndex) -> ExecutorResult<Self> {
        let CreateIndex {
            name,
            table_name,
            column_indexes,
            is_unique,
        } = plan;
        ctx.catalog()
            .create_index(name, table_name, &column_indexes, is_unique)?;
        Ok(Self::empty_result())
    }

    pub fn drop_object(ctx: &ConnectionContext, plan: DropObject) -> ExecutorResult<Self> {
        let catalog = ctx.catalog();
        let result = match plan.0.kind {
            ObjectKind::Table => catalog.drop_table(&plan.0.name),
            ObjectKind::Index => catalog.drop_index(&plan.0.name),
        };
        match result {
            Ok(()) => (),
            Err(CatalogError::UnknownEntry(_, _)) if plan.0.if_exists => (),
            Err(e) => return Err(e.into()),
        }
        Ok(Self::empty_result())
    }

    pub fn truncate(plan: Truncate) -> ExecutorResult<Self> {
        let Truncate { table } = plan;
        table.truncate()?;
        Ok(Self::empty_result())
    }

    pub fn reindex(plan: Reindex) -> ExecutorResult<Self> {
        match plan {
            Reindex::Table(table) => table.reindex()?,
            Reindex::Index(index) => index.reindex()?,
        }
        Ok(Self::empty_result())
    }
}

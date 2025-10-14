pub mod serde_json_to_sea_orm_vals;

use std::{
    marker::PhantomData,
    sync::{Arc, atomic::AtomicBool},
};

use async_trait::async_trait;
use cruding_core::{
    Crudable, CrudableSource, UpdateComparingParams,
    list::{CrudableSourceListExt, CrudingListParams, CrudingListSortOrder},
};
use sea_orm::{
    DatabaseConnection, DatabaseTransaction, DbErr, EntityTrait, FromQueryResult, IntoActiveModel,
    Iterable, ModelTrait, QueryOrder, QuerySelect, SelectColumns, Statement, TransactionTrait,
    TryGetableMany, prelude::*, sea_query::IntoCondition,
};
use tokio::sync::RwLock;

use crate::serde_json_to_sea_orm_vals::{json_to_value_for_column, json_to_value_for_column_arr};

pub trait PostgresCrudableTable: EntityTrait
where
    Self::Model: Crudable,
    Self::Column: Iterable + PartialEq,
{
    fn get_pkey_filter(keys: &[<Self::Model as Crudable>::Pkey]) -> impl IntoCondition;
    /// Returns a vector with the primary key columns
    fn get_pkey_columns() -> Vec<Self::Column>;
}

pub struct CrudablePostgresSource<
    CRUDTable: PostgresCrudableTable,
    Ctx: Send + Sync + 'static,
    Error: From<sea_orm::DbErr>,
> where
    <CRUDTable as EntityTrait>::Column: Iterable + PartialEq,
    <CRUDTable as EntityTrait>::Model: Crudable
        + ModelTrait<Entity = CRUDTable>
        + IntoActiveModel<<CRUDTable as EntityTrait>::ActiveModel>
        + FromQueryResult,
{
    conn: DatabaseConnection,
    lock_for_update: Arc<AtomicBool>,
    _p: PhantomData<(CRUDTable, Ctx, Error)>,
}

impl<CRUDTable: PostgresCrudableTable, Ctx: Send + Sync + 'static, Error: From<sea_orm::DbErr>>
    Clone for CrudablePostgresSource<CRUDTable, Ctx, Error>
where
    <CRUDTable as EntityTrait>::Column: Iterable + PartialEq,
    <CRUDTable as EntityTrait>::Model: Crudable
        + ModelTrait<Entity = CRUDTable>
        + IntoActiveModel<<CRUDTable as EntityTrait>::ActiveModel>
        + FromQueryResult,
{
    fn clone(&self) -> Self {
        Self {
            conn: self.conn.clone(),
            lock_for_update: self.lock_for_update.clone(),
            _p: self._p,
        }
    }
}

#[derive(Clone)]
pub struct PostgresCrudableConnection {
    conn: Arc<RwLock<PostgresCrudableConnectionInner>>,
}

impl PostgresCrudableConnection {
    pub fn new(conn: PostgresCrudableConnectionInner) -> Self {
        Self {
            conn: Arc::new(RwLock::new(conn)),
        }
    }

    pub fn new_from_conn(conn: DatabaseConnection) -> Self {
        Self {
            conn: Arc::new(RwLock::new(PostgresCrudableConnectionInner::Connection(
                conn,
            ))),
        }
    }

    pub fn get_conn(&self) -> &RwLock<PostgresCrudableConnectionInner> {
        self.conn.as_ref()
    }
}

pub enum PostgresCrudableConnectionInner {
    /// Represents an owned connection by the context that wasn't requested to start a
    /// transaction
    Connection(DatabaseConnection),
    /// Represents an owned connection by the context that was requested to start a
    /// transaction
    OwnedTransaction(DatabaseConnection, Arc<DatabaseTransaction>),
    /// Represents a "borrowed" connection by the conext that comes from a caller that already has
    /// a transaction underway
    BorrowedTransaction(Arc<DatabaseTransaction>),
}

impl PostgresCrudableConnectionInner {
    pub fn is_transaction(&self) -> bool {
        !matches!(self, Self::Connection(_))
    }

    /// Will begin a transaction if the connection is owned by the context
    pub async fn maybe_begin_transaction(&mut self) -> Result<(), DbErr> {
        if let Self::Connection(c) = self {
            *self = Self::OwnedTransaction(c.clone(), Arc::new(c.begin().await?));
        }

        Ok(())
    }

    /// Will commit if connection is owned transaction
    pub async fn maybe_commit(&mut self) -> Result<(), DbErr> {
        let mut conn = None;

        if let Self::OwnedTransaction(c, _) = self {
            conn = Some(c.clone());
        };

        if let Some(conn) = conn {
            let Self::OwnedTransaction(_, tx) = std::mem::replace(self, Self::Connection(conn))
            else {
                unreachable!()
            };
            Arc::try_unwrap(tx).map_err(|_| DbErr::Custom("Failed to finish an OwnedTransaction this means something still holds a reference to it...".to_string()))?.
            commit().await?;
        }

        Ok(())
    }

    pub async fn get_conn(&self) -> &(dyn ConnectionTrait + Send + Sync) {
        match self {
            Self::Connection(c) => c,
            Self::OwnedTransaction(_, tx) => tx.as_ref(),
            Self::BorrowedTransaction(tx) => tx.as_ref(),
        }
    }
}

impl<CRUDTable, Ctx, Error> CrudablePostgresSource<CRUDTable, Ctx, Error>
where
    CRUDTable: PostgresCrudableTable,
    CRUDTable::Model: Crudable
        + ModelTrait<Entity = CRUDTable>
        + IntoActiveModel<<CRUDTable as EntityTrait>::ActiveModel>
        + FromQueryResult,
    CRUDTable::Column: Iterable + PartialEq,
    CRUDTable::ActiveModel: Send,
    Error: From<sea_orm::DbErr> + Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
{
    pub fn new(conn: DatabaseConnection, lock_for_update: bool) -> Self {
        Self {
            conn,
            lock_for_update: Arc::new(lock_for_update.into()),
            _p: PhantomData,
        }
    }

    pub fn set_connection(&mut self, conn: DatabaseConnection) {
        self.conn = conn;
    }

    pub fn new_source_handle(&self) -> PostgresCrudableConnection {
        PostgresCrudableConnection {
            conn: Arc::new(RwLock::new(PostgresCrudableConnectionInner::Connection(
                self.conn.clone(),
            ))),
        }
    }
}

#[async_trait]
impl<CRUDTable, SourceHandle, Error> CrudableSource<<CRUDTable as EntityTrait>::Model>
    for CrudablePostgresSource<CRUDTable, SourceHandle, Error>
where
    CRUDTable: PostgresCrudableTable,
    CRUDTable::Model: Crudable
        + ModelTrait<Entity = CRUDTable>
        + IntoActiveModel<<CRUDTable as EntityTrait>::ActiveModel>
        + FromQueryResult,
    CRUDTable::Column: Iterable + PartialEq,
    CRUDTable::ActiveModel: Send,
    Error: From<sea_orm::DbErr> + Send + Sync + 'static,
    SourceHandle: Clone + Send + Sync + 'static,
{
    type Error = Error;
    type SourceHandle = PostgresCrudableConnection;

    #[tracing::instrument(skip_all)]
    async fn create(
        &self,
        items: Vec<<CRUDTable as EntityTrait>::Model>,
        handle: Self::SourceHandle,
    ) -> Result<Vec<<CRUDTable as EntityTrait>::Model>, Self::Error> {
        let active_models: Vec<<CRUDTable as EntityTrait>::ActiveModel> = items
            .into_iter()
            .map(IntoActiveModel::into_active_model)
            .collect();

        let q = CRUDTable::insert_many(active_models);

        let returned_items = match &*handle.conn.read().await {
            PostgresCrudableConnectionInner::Connection(c) => q.exec_with_returning_many(c).await,
            PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => {
                q.exec_with_returning_many(tx.as_ref()).await
            }
            PostgresCrudableConnectionInner::BorrowedTransaction(tx) => {
                q.exec_with_returning_many(tx.as_ref()).await
            }
        }?;

        Ok(returned_items)
    }

    #[tracing::instrument(skip_all)]
    async fn read(
        &self,
        keys: &[<<CRUDTable as EntityTrait>::Model as Crudable>::Pkey],
        handle: Self::SourceHandle,
    ) -> Result<Vec<<CRUDTable as EntityTrait>::Model>, Self::Error> {
        let q =
            CRUDTable::find().filter(<CRUDTable as PostgresCrudableTable>::get_pkey_filter(keys));

        let returned_items = match &*handle.conn.read().await {
            PostgresCrudableConnectionInner::Connection(c) => q.all(c).await,
            PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => q.all(tx.as_ref()).await,
            PostgresCrudableConnectionInner::BorrowedTransaction(tx) => q.all(tx.as_ref()).await,
        }?;

        Ok(returned_items)
    }

    #[tracing::instrument(skip_all)]
    async fn update(
        &self,
        items: UpdateComparingParams<<CRUDTable as EntityTrait>::Model>,
        handle: Self::SourceHandle,
    ) -> Result<Vec<<CRUDTable as EntityTrait>::Model>, Self::Error> {
        if items.update_payload.is_empty() {
            return Ok(Vec::new());
        }

        // 1) Resolve table & columns from the entity
        let table = CRUDTable::default();
        let table_name = format!(r#""{}""#, table.table_name());
        let pk_cols: Vec<<CRUDTable as EntityTrait>::Column> =
            <CRUDTable as PostgresCrudableTable>::get_pkey_columns();
        let all_cols: Vec<<CRUDTable as EntityTrait>::Column> =
            <CRUDTable as EntityTrait>::Column::iter().collect();
        let updatable_cols: Vec<_> = all_cols
            .iter()
            .cloned()
            .filter(|c| !pk_cols.contains(c))
            .collect();

        // If nothing is updatable (edge case), just return current rows for these keys
        if updatable_cols.is_empty() {
            let keys = items
                .update_payload
                .iter()
                .map(Crudable::pkey)
                .collect::<Vec<_>>();
            return self.read(&keys, handle).await;
        }

        // v(pk1, pk2, ..., col1, col2, ...)
        let mut v_cols: Vec<String> = Vec::with_capacity(pk_cols.len() + updatable_cols.len());
        v_cols.extend(pk_cols.iter().map(|c| c.to_string()));
        v_cols.extend(updatable_cols.iter().map(|c| c.to_string()));
        let v_cols_sql = format!("({})", v_cols.join(", "));

        // SET t.col = v.col
        let set_sql = updatable_cols
            .iter()
            .map(|c| {
                let id = c.to_string();
                format!("{} = v.{}", id, id)
            })
            .collect::<Vec<_>>()
            .join(", ");

        // WHERE t.pkX = v.pkX AND ...
        let where_sql = pk_cols
            .iter()
            .map(|c| {
                let id = c.to_string();
                format!("t.{} = v.{}", id, id)
            })
            .collect::<Vec<_>>()
            .join(" AND ");

        // 2) Build VALUES list as placeholders with bind params
        // Row shape: (pk1, pk2, ..., col1, col2, ...)
        let total_cols = pk_cols.len() + updatable_cols.len();
        let mut bind_params: Vec<Value> =
            Vec::with_capacity(items.update_payload.len() * total_cols);

        // helper to make "($1, $2, ... $N)" for a given starting index
        let mut next_idx: usize = 1;
        let mut rows_sql: Vec<String> = Vec::with_capacity(items.update_payload.len());
        for m in items.update_payload {
            let am = m.into_active_model();

            // push PKs in declared order
            for c in &pk_cols {
                bind_params.push(am.get(*c).into_value().unwrap());
            }
            // push updatable columns in declared order
            for c in &updatable_cols {
                bind_params.push(am.get(*c).into_value().unwrap());
            }

            // TODO: pre alloc string, this does too many format!()
            let row_placeholders = (0..total_cols)
                .map(|i| format!("${}", next_idx + i))
                .collect::<Vec<_>>()
                .join(", ");
            next_idx += total_cols;
            rows_sql.push(format!("({})", row_placeholders));
        }

        // 3) Final SQL with only identifiers interpolated, all values bound
        let sql = format!(
            "UPDATE {table} AS t \
         SET {set_clause} \
         FROM (VALUES {values_rows}) AS v {vcols} \
         WHERE {where_clause} \
         RETURNING t.*;",
            table = table_name,
            set_clause = set_sql,
            values_rows = rows_sql.join(", "),
            vcols = v_cols_sql,
            where_clause = where_sql,
        );

        let stmt =
            Statement::from_sql_and_values(sea_orm::DatabaseBackend::Postgres, sql, bind_params);

        // 4) Execute & map rows to CRUD
        async fn run<R, M>(exec: &R, stmt: Statement) -> Result<Vec<M>, sea_orm::DbErr>
        where
            R: ConnectionTrait,
            M: FromQueryResult,
        {
            let rows = exec.query_all(stmt).await?;
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                out.push(M::from_query_result(&row, "")?);
            }
            Ok(out)
        }

        let returned = match &*handle.conn.read().await {
            PostgresCrudableConnectionInner::Connection(c) => run(c, stmt).await?,
            PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => {
                run(tx.as_ref(), stmt).await?
            }
            PostgresCrudableConnectionInner::BorrowedTransaction(tx) => {
                run(tx.as_ref(), stmt).await?
            }
        };

        Ok(returned)
    }
    #[tracing::instrument(skip_all)]
    async fn read_for_update(
        &self,
        keys: &[<<CRUDTable as EntityTrait>::Model as Crudable>::Pkey],
        handle: Self::SourceHandle,
    ) -> Result<Vec<Arc<<CRUDTable as EntityTrait>::Model>>, Self::Error> {
        let mut q =
            CRUDTable::find().filter(<CRUDTable as PostgresCrudableTable>::get_pkey_filter(keys));

        // avoid deadlocks with multi batch updates
        for col in CRUDTable::get_pkey_columns() {
            q = q.order_by_asc(col);
        }

        let mut handle = handle.conn.write().await;

        if self
            .lock_for_update
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            handle.maybe_begin_transaction().await?;

            if handle.is_transaction() {
                q = q.lock_exclusive()
            }
        }

        let returned_items = match &*handle {
            PostgresCrudableConnectionInner::Connection(c) => q.all(c).await,
            PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => q.all(tx.as_ref()).await,
            PostgresCrudableConnectionInner::BorrowedTransaction(tx) => q.all(tx.as_ref()).await,
        }?
        .into_iter()
        .map(Arc::new)
        .collect();

        Ok(returned_items)
    }

    #[tracing::instrument(skip_all)]
    async fn delete(
        &self,
        keys: &[<<CRUDTable as EntityTrait>::Model as Crudable>::Pkey],
        handle: Self::SourceHandle,
    ) -> Result<Vec<<CRUDTable as EntityTrait>::Model>, Self::Error> {
        let q = CRUDTable::delete_many()
            .filter(<CRUDTable as PostgresCrudableTable>::get_pkey_filter(keys));

        let returned_items = match &*handle.conn.read().await {
            PostgresCrudableConnectionInner::Connection(c) => q.exec_with_returning(c).await,
            PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => {
                q.exec_with_returning(tx.as_ref()).await
            }
            PostgresCrudableConnectionInner::BorrowedTransaction(tx) => {
                q.exec_with_returning(tx.as_ref()).await
            }
        }?;

        Ok(returned_items)
    }

    async fn should_use_cache(&self, handle: Self::SourceHandle) -> bool {
        !handle.conn.read().await.is_transaction()
    }

    async fn can_use_batcher(&self, handle: Self::SourceHandle) -> bool {
        self.should_use_cache(handle).await
    }
}

#[async_trait]
impl<CRUDTable, SourceHandle, Error>
    CrudableSourceListExt<<CRUDTable as EntityTrait>::Model, <CRUDTable as EntityTrait>::Column>
    for CrudablePostgresSource<CRUDTable, SourceHandle, Error>
where
    CRUDTable: PostgresCrudableTable,
    CRUDTable::Model: Crudable
        + ModelTrait<Entity = CRUDTable>
        + IntoActiveModel<<CRUDTable as EntityTrait>::ActiveModel>
        + FromQueryResult,
    <CRUDTable::Model as Crudable>::Pkey: TryGetableMany,
    CRUDTable::Column: Iterable + PartialEq,
    CRUDTable::ActiveModel: Send,
    Error: From<sea_orm::DbErr> + Send + Sync + 'static,
    SourceHandle: Clone + Send + Sync + 'static,
{
    async fn read_list_to_ids(
        &self,
        params: CrudingListParams<CRUDTable::Column>,
        handle: Self::SourceHandle,
    ) -> Result<Vec<<CRUDTable::Model as Crudable>::Pkey>, Self::Error> {
        let mut query = CRUDTable::find().select_only();
        for col in CRUDTable::get_pkey_columns() {
            query = query.select_column(col);
        }

        for filter in params.filters {
            use cruding_core::list::CrudingListFilterOperators::*;

            query = match filter.op {
                Eq(v) => query.filter(ColumnTrait::eq(
                    &filter.column,
                    json_to_value_for_column(&filter.column, v).unwrap(),
                )),
                Neq(v) => query.filter(ColumnTrait::ne(
                    &filter.column,
                    json_to_value_for_column(&filter.column, v).unwrap(),
                )),
                Gt(v) => query.filter(ColumnTrait::gt(
                    &filter.column,
                    json_to_value_for_column(&filter.column, v).unwrap(),
                )),
                Ge(v) => query.filter(ColumnTrait::gte(
                    &filter.column,
                    json_to_value_for_column(&filter.column, v).unwrap(),
                )),
                Lt(v) => query.filter(ColumnTrait::lt(
                    &filter.column,
                    json_to_value_for_column(&filter.column, v).unwrap(),
                )),
                Le(v) => query.filter(ColumnTrait::lte(
                    &filter.column,
                    json_to_value_for_column(&filter.column, v).unwrap(),
                )),
                In(v) => query.filter(ColumnTrait::is_in(
                    &filter.column,
                    json_to_value_for_column_arr(&filter.column, v).unwrap(),
                )),
                NotIn(v) => query.filter(ColumnTrait::is_not_in(
                    &filter.column,
                    json_to_value_for_column_arr(&filter.column, v).unwrap(),
                )),
            }
        }

        for sort in params.sorts {
            query = match sort.order {
                CrudingListSortOrder::Asc => query.order_by_asc(sort.column),
                CrudingListSortOrder::Desc => query.order_by_desc(sort.column),
            }
        }

        let query = query.into_tuple::<<CRUDTable::Model as Crudable>::Pkey>();

        let returned_items = match &*handle.conn.read().await {
            PostgresCrudableConnectionInner::Connection(c) => {
                query
                    .paginate(c, params.pagination.size as _)
                    .fetch_page(params.pagination.page as _)
                    .await
            }
            PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => {
                query
                    .paginate(tx.as_ref(), params.pagination.size as _)
                    .fetch_page(params.pagination.page as _)
                    .await
            }
            PostgresCrudableConnectionInner::BorrowedTransaction(tx) => {
                query
                    .paginate(tx.as_ref(), params.pagination.size as _)
                    .fetch_page(params.pagination.page as _)
                    .await
            }
        }?;

        Ok(returned_items)
    }
}

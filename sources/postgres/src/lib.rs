use std::{marker::PhantomData, sync::Arc};

use async_trait::async_trait;
use cruding_core::{Crudable, CrudableSource};
use sea_orm::{
    DatabaseConnection, DatabaseTransaction, DbErr, EntityTrait, FromQueryResult, IntoActiveModel,
    ModelTrait, QuerySelect, TransactionTrait, prelude::*, sea_query::IntoCondition,
};

pub trait PostgresCrudableTable: EntityTrait
where
    <Self as EntityTrait>::Model: Crudable,
{
    fn get_pkey_filter(
        keys: &[<<Self as EntityTrait>::Model as Crudable>::Pkey],
    ) -> impl IntoCondition;
}

pub struct CrudablePostgresSource<
    CRUD: Crudable
        + ModelTrait<Entity = CRUDTable>
        + IntoActiveModel<<CRUDTable as EntityTrait>::ActiveModel>
        + FromQueryResult,
    CRUDTable: PostgresCrudableTable<Model = CRUD>,
    Ctx: Send + Sync + 'static,
    Error: From<sea_orm::DbErr>,
> {
    conn: DatabaseConnection,
    lock_for_update: bool,
    _p: PhantomData<(CRUD, CRUDTable, Ctx, Error)>,
}

pub enum PostgresCrudableConnection {
    /// Represents an owned connection by the context that wasn't requested to start a
    /// transaction
    Connection(DatabaseConnection),
    /// Represents an owned connection by the context that was requested to start a
    /// transaction
    OwnedTransaction(DatabaseConnection, DatabaseTransaction),
    /// Represents a "borrowed" connection by the conext that comes from a caller that already has
    /// a transaction underway
    BorrowedTransaction(Arc<DatabaseTransaction>),
}

impl PostgresCrudableConnection {
    pub fn is_transaction(&self) -> bool {
        !matches!(self, Self::Connection(_))
    }

    /// Will begin a transaction if the connection is owned by the context
    pub async fn maybe_begin_transaction(&mut self) -> Result<(), DbErr> {
        if let PostgresCrudableConnection::Connection(c) = self {
            *self = Self::OwnedTransaction(c.clone(), c.begin().await?);
        }

        Ok(())
    }

    /// Will commit if connection is owned transaction
    pub async fn maybe_commit(&mut self) -> Result<(), DbErr> {
        let mut conn = None;

        if let PostgresCrudableConnection::OwnedTransaction(c, _) = self {
            conn = Some(c.clone());
        };

        if let Some(conn) = conn {
            let Self::OwnedTransaction(_, tx) = std::mem::replace(self, Self::Connection(conn))
            else {
                unreachable!()
            };
            tx.commit().await?;
        }

        Ok(())
    }
}

impl<CRUD, CRUDTable, Ctx, Error> CrudablePostgresSource<CRUD, CRUDTable, Ctx, Error>
where
    CRUD: Crudable
        + ModelTrait<Entity = CRUDTable>
        + IntoActiveModel<<CRUDTable as EntityTrait>::ActiveModel>
        + FromQueryResult,
    CRUDTable: PostgresCrudableTable<Model = CRUD>,
    Error: From<sea_orm::DbErr> + Send + Sync + 'static,
    Ctx: Send + Sync + 'static,
{
    pub fn new(conn: DatabaseConnection, lock_for_update: bool) -> Self {
        Self {
            conn,
            lock_for_update,
            _p: PhantomData,
        }
    }

    pub fn set_connection(&mut self, conn: DatabaseConnection) {
        self.conn = conn;
    }

    pub fn new_handle(&self) -> PostgresCrudableConnection {
        PostgresCrudableConnection::Connection(self.conn.clone())
    }
}

#[async_trait]
impl<CRUD, CRUDTable, SourceHandle, Error> CrudableSource<CRUD>
    for CrudablePostgresSource<CRUD, CRUDTable, SourceHandle, Error>
where
    CRUD: Crudable
        + ModelTrait<Entity = CRUDTable>
        + IntoActiveModel<<CRUDTable as EntityTrait>::ActiveModel>
        + FromQueryResult,
    CRUDTable: PostgresCrudableTable<Model = CRUD>,
    Error: From<sea_orm::DbErr> + Send + Sync + 'static,
    SourceHandle: Send + Sync + 'static,
{
    type Error = Error;
    type SourceHandle = PostgresCrudableConnection;

    #[tracing::instrument(skip_all)]
    async fn create(
        &self,
        items: Vec<CRUD>,
        handle: &mut Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error> {
        let active_models: Vec<<CRUDTable as EntityTrait>::ActiveModel> = items
            .into_iter()
            .map(IntoActiveModel::into_active_model)
            .collect();

        let q = <CRUD as ModelTrait>::Entity::insert_many(active_models);

        let returned_items = match handle {
            PostgresCrudableConnection::Connection(c) => q.exec_with_returning_many(c).await,
            PostgresCrudableConnection::OwnedTransaction(_, tx) => {
                q.exec_with_returning_many(tx).await
            }
            PostgresCrudableConnection::BorrowedTransaction(tx) => {
                q.exec_with_returning_many(tx.as_ref()).await
            }
        }?;

        Ok(returned_items)
    }

    #[tracing::instrument(skip_all)]
    async fn read(
        &self,
        keys: &[CRUD::Pkey],
        handle: &mut Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error> {
        let q = <CRUD as ModelTrait>::Entity::find()
            .filter(<CRUDTable as PostgresCrudableTable>::get_pkey_filter(keys));

        let returned_items = match handle {
            PostgresCrudableConnection::Connection(c) => q.all(c).await,
            PostgresCrudableConnection::OwnedTransaction(_, tx) => q.all(tx).await,
            PostgresCrudableConnection::BorrowedTransaction(tx) => q.all(tx.as_ref()).await,
        }?;

        Ok(returned_items)
    }

    #[tracing::instrument(skip_all)]
    async fn update(
        &self,
        items: Vec<CRUD>,
        handle: &mut Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error> {
        let active_models: Vec<<CRUDTable as EntityTrait>::ActiveModel> = items
            .into_iter()
            .map(IntoActiveModel::into_active_model)
            .collect();

        let mut q = <CRUD as ModelTrait>::Entity::update_many();

        for am in active_models {
            q = q.set(am);
        }

        let returned_items = match handle {
            PostgresCrudableConnection::Connection(c) => q.exec_with_returning(c).await,
            PostgresCrudableConnection::OwnedTransaction(_, tx) => q.exec_with_returning(tx).await,
            PostgresCrudableConnection::BorrowedTransaction(tx) => {
                q.exec_with_returning(tx.as_ref()).await
            }
        }?;

        Ok(returned_items)
    }

    #[tracing::instrument(skip_all)]
    async fn read_for_update(
        &self,
        keys: &[CRUD::Pkey],
        handle: &mut Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error> {
        let mut q = <CRUD as ModelTrait>::Entity::find()
            .filter(<CRUDTable as PostgresCrudableTable>::get_pkey_filter(keys));

        if self.lock_for_update {
            handle.maybe_begin_transaction().await?;

            if handle.is_transaction() {
                q = q.lock_exclusive()
            }
        }

        let returned_items = match handle {
            PostgresCrudableConnection::Connection(c) => q.all(c).await,
            PostgresCrudableConnection::OwnedTransaction(_, tx) => q.all(tx).await,
            PostgresCrudableConnection::BorrowedTransaction(tx) => q.all(tx.as_ref()).await,
        }?;

        handle.maybe_commit().await?;

        Ok(returned_items)
    }

    #[tracing::instrument(skip_all)]
    async fn delete(
        &self,
        keys: &[CRUD::Pkey],
        handle: &mut Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error> {
        let q = <CRUD as ModelTrait>::Entity::delete_many()
            .filter(<CRUDTable as PostgresCrudableTable>::get_pkey_filter(keys));

        let returned_items = match handle {
            PostgresCrudableConnection::Connection(c) => q.exec_with_returning(c).await,
            PostgresCrudableConnection::OwnedTransaction(_, tx) => q.exec_with_returning(tx).await,
            PostgresCrudableConnection::BorrowedTransaction(tx) => {
                q.exec_with_returning(tx.as_ref()).await
            }
        }?;

        Ok(returned_items)
    }

    fn use_cache(&self, handle: &Self::SourceHandle) -> bool {
        handle.is_transaction()
    }
}

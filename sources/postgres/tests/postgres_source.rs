use std::sync::Arc;

use cruding_core::{Crudable, CrudableSource};
use cruding_pg_source::{
    CrudablePostgresSource, PostgresCrudableConnection, PostgresCrudableConnectionInner, PostgresCrudableTable
};

use sea_orm::{
    Database, DatabaseBackend, DatabaseConnection, Iterable, Schema, Statement, TransactionTrait,
    entity::prelude::*,
    sea_query::{IntoCondition, PostgresQueryBuilder},
};
use serial_test::serial;

// --------- config ---------

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_string())
}

// --------- entity used only in tests ---------

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "items")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub mono: i64,
    pub val: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl Crudable for Model {
    type Pkey = i32;
    type MonoField = i64;
    fn pkey(&self) -> Self::Pkey {
        self.id
    }
    fn mono_field(&self) -> Self::MonoField {
        self.mono
    }
}

impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

impl PostgresCrudableTable for Entity
where
    <Self as EntityTrait>::Model: Crudable,
    <Self as EntityTrait>::Column: Iterable + PartialEq,
{
    fn get_pkey_filter(keys: &[<Model as Crudable>::Pkey]) -> impl IntoCondition {
        Column::Id.is_in(keys.to_vec())
    }

    fn get_pkey_columns() -> Vec<Self::Column> {
        vec![Column::Id]
    }
}

// --------- helpers ---------

async fn connect_and_prepare() -> DatabaseConnection {
    let conn = Database::connect(&db_url())
        .await
        .expect("connect postgres");

    // Create table if missing
    let schema = Schema::new(DatabaseBackend::Postgres);
    let stmt = schema.create_table_from_entity(Entity);
    conn.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        stmt.to_string(PostgresQueryBuilder),
    ))
    .await
    .ok(); // ignore if already exists

    // Truncate between tests
    conn.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        "TRUNCATE TABLE items RESTART IDENTITY".to_string(),
    ))
    .await
    .expect("truncate items");

    conn
}

fn source(
    lock_for_update: bool,
    conn: DatabaseConnection,
) -> CrudablePostgresSource<Entity, (), LiveErr> {
    CrudablePostgresSource::new(conn, lock_for_update)
}

#[derive(thiserror::Error, Debug)]
pub enum LiveErr {
    #[error(transparent)]
    Db(#[from] sea_orm::DbErr),
}

// --------- tests ---------
// Serialize tests to avoid table conflicts; remove #[serial] if you manage isolation differently.

#[tokio::test]
#[serial]
async fn create_returns_inserted_rows() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn);

    let handle = src.new_source_handle();
    let rows = vec![
        Model {
            id: 1,
            mono: 10,
            val: 111,
        },
        Model {
            id: 2,
            mono: 20,
            val: 222,
        },
    ];

    let out = CrudableSource::<Model>::create(&src, rows.clone(), handle)
        .await
        .unwrap();
    assert_eq!(out, rows);
}

#[tokio::test]
#[serial]
async fn read_filters_by_keys() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());
    let h = src.new_source_handle();

    let seed = vec![
        Model {
            id: 1,
            mono: 10,
            val: 111,
        },
        Model {
            id: 2,
            mono: 20,
            val: 222,
        },
        Model {
            id: 3,
            mono: 30,
            val: 333,
        },
    ];
    CrudableSource::<Model>::create(&src, seed, h)
        .await
        .unwrap();

    let handle = src.new_source_handle();
    let out = CrudableSource::<Model>::read(&src, &[1, 3], handle)
        .await
        .unwrap();
    assert_eq!(out.len(), 2);
    assert!(out.iter().any(|m| m.id == 1));
    assert!(out.iter().any(|m| m.id == 3));
}

#[tokio::test]
#[serial]
async fn update_many_returns_updated() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());
    let h = src.new_source_handle();

    let seed = vec![
        Model {
            id: 1,
            mono: 1,
            val: 10,
        },
        Model {
            id: 2,
            mono: 1,
            val: 20,
        },
    ];
    CrudableSource::<Model>::create(&src, seed, h)
        .await
        .unwrap();

    let updated = vec![
        Model {
            id: 1,
            mono: 2,
            val: 999,
        },
        Model {
            id: 2,
            mono: 3,
            val: 888,
        },
    ];

    let handle = src.new_source_handle();
    let out = CrudableSource::<Model>::update(&src, updated.clone(), handle)
        .await
        .unwrap();
    assert_eq!(out, updated);
}

#[tokio::test]
#[serial]
async fn delete_returns_deleted_rows() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());
    let h = src.new_source_handle();

    let seed = vec![
        Model {
            id: 2,
            mono: 2,
            val: 22,
        },
        Model {
            id: 4,
            mono: 4,
            val: 44,
        },
    ];
    CrudableSource::<Model>::create(&src, seed.clone(), h)
        .await
        .unwrap();

    let handle = src.new_source_handle();
    let out = CrudableSource::<Model>::delete(&src, &[2, 4], handle)
        .await
        .unwrap();
    // We expect the deleted rows to be returned (SeaORM's returning)
    assert_eq!(out.len(), 2);
    assert!(out.iter().any(|m| m.id == 2));
    assert!(out.iter().any(|m| m.id == 4));
}

#[tokio::test]
#[serial]
async fn read_for_update_owned_and_borrowed_behave() {
    // lock_for_update = true
    let conn = connect_and_prepare().await;
    let src = source(true, conn.clone());

    // Seed
    let h_seed = src.new_source_handle();
    CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 7,
            mono: 70,
            val: 700,
        }],
        h_seed,
    )
    .await
    .unwrap();

    // Case A: plain connection -> source begins & commits internally; handle ends as Connection
    let h1 = src.new_source_handle();
    let rows1 = CrudableSource::<Model>::read_for_update(&src, &[7], h1.clone())
        .await
        .unwrap();
    assert_eq!(rows1[0].id, 7);
    match &*h1.get_conn().read().await {
        PostgresCrudableConnectionInner::Connection(_) => {}
        _ => panic!("expected Connection after auto-commit"),
    }

    // Case B: owned tx -> remains our responsibility, but your impl auto-commits in read_for_update;
    // verify handle reset to Connection
    let h2 = src.new_source_handle();
    h2.get_conn().write().await.maybe_begin_transaction().await.unwrap();
    let rows2 = CrudableSource::<Model>::read_for_update(&src, &[7], h2.clone())
        .await
        .unwrap();
    assert_eq!(rows2[0].id, 7);
    match &*h2.get_conn().read().await {
        PostgresCrudableConnectionInner::Connection(_) => {}
        _ => panic!("expected Connection after auto-commit of owned tx"),
    }

    // Case C: borrowed tx -> must NOT be committed by the source; handle stays Borrowed
    let tx = conn.begin().await.unwrap();
    let h3 = PostgresCrudableConnection::new(PostgresCrudableConnectionInner::BorrowedTransaction(Arc::new(tx)));
    let rows3 = CrudableSource::<Model>::read_for_update(&src, &[7], h3.clone())
        .await
        .unwrap();
    assert_eq!(rows3[0].id, 7);
    match &*h3.get_conn().read().await {
        PostgresCrudableConnectionInner::BorrowedTransaction(_) => {}
        _ => panic!("borrowed tx must remain borrowed"),
    }
}

#[tokio::test]
#[serial]
async fn use_cache_policy_is_false_inside_tx_true_otherwise() {
    let conn = connect_and_prepare().await;
    let src = source(false, conn.clone());

    // plain connection => use cache
    let h1 = src.new_source_handle();
    assert!(CrudableSource::<Model>::should_use_cache(&src, h1).await);

    // owned transaction => bypass cache
    let h2 = src.new_source_handle();
    h2.get_conn().write().await.maybe_begin_transaction().await.unwrap();
    assert!(!CrudableSource::<Model>::should_use_cache(&src, h2).await);

    // borrowed transaction => bypass cache
    let tx = conn.begin().await.unwrap();
    let h3 = PostgresCrudableConnection::new(PostgresCrudableConnectionInner::BorrowedTransaction(Arc::new(tx)));
    assert!(!CrudableSource::<Model>::should_use_cache(&src, h3).await);
}

#[tokio::test]
#[serial]
async fn end_to_end_inside_owned_tx() {
    let conn = connect_and_prepare().await;
    let src = source(true, conn);

    let h = src.new_source_handle();
    h.get_conn().write().await.maybe_begin_transaction().await.unwrap();

    // create
    let c = CrudableSource::<Model>::create(
        &src,
        vec![Model {
            id: 10,
            mono: 1,
            val: 10,
        }],
        h.clone(),
    )
    .await
    .unwrap();
    assert_eq!(c[0].id, 10);

    // read
    let r = CrudableSource::<Model>::read(&src, &[10], h.clone())
        .await
        .unwrap();
    assert_eq!(r[0].val, 10);

    // update
    let u = CrudableSource::<Model>::update(
        &src,
        vec![Model {
            id: 10,
            mono: 2,
            val: 20,
        }],
        h.clone(),
    )
    .await
    .unwrap();
    assert_eq!(u[0].mono, 2);

    // delete
    let d = CrudableSource::<Model>::delete(&src, &[10], h.clone())
        .await
        .unwrap();
    assert_eq!(d[0].id, 10);

    // commit (handle should reset to Connection)
    h.get_conn().write().await.maybe_commit().await.unwrap();
    match &*h.get_conn().read().await {
        PostgresCrudableConnectionInner::Connection(_) => {}
        _ => panic!("expected Connection after explicit commit"),
    }
}

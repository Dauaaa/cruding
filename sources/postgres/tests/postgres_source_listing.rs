// tests/filter_postgres.rs
#![allow(clippy::disallowed_types, clippy::float_cmp)]

use std::sync::Arc;

use cruding_core::{
    Crudable,
    list::{
        CrudableSourceListExt, CrudingListFilter, CrudingListFilterOperators as Op,
        CrudingListPagination, CrudingListParams, CrudingListSortOrder,
    },
};
use cruding_pg_source::{
    // <-- change to your crate name
    CrudablePostgresSource, PostgresCrudableConnection, PostgresCrudableConnectionInner, PostgresCrudableTable
};

use sea_orm::{
    Database, DatabaseBackend, DatabaseConnection, IntoActiveModel, Iterable, QueryOrder, Schema,
    Statement, TransactionTrait,
    entity::prelude::*,
    sea_query::{IntoCondition, PostgresQueryBuilder},
};
use serde_json::json;
use serial_test::serial;
use uuid::Uuid;

// ---------- config ----------

fn db_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postgres".to_string())
}

// ---------- test entity (Postgres) ----------

#[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
#[sea_orm(table_name = "items_filter")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
    pub mono: i64,

    // Fields we will filter on:
    pub i: i32,                    // integer
    pub s: String,                 // text
    pub uid: Uuid,                 // uuid
    pub dt: chrono::NaiveDateTime, // datetime
    pub js: serde_json::Value,     // json
    pub ia: Option<Vec<i32>>, // int array (not used by ops directly; present to ensure schema ok)
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

// SeaORM Column doesn't implement PartialEq by default—use discriminant compare
impl PartialEq for Column {
    fn eq(&self, other: &Self) -> bool {
        core::mem::discriminant(self) == core::mem::discriminant(other)
    }
}

// Hook the entity into your PostgresCrudableTable
impl PostgresCrudableTable for Entity
where
    <Self as EntityTrait>::Model: Crudable,
    <Self as EntityTrait>::Column: Iterable + PartialEq,
{
    fn get_pkey_filter(keys: &[<Self::Model as Crudable>::Pkey]) -> impl IntoCondition {
        Column::Id.is_in(keys.to_vec())
    }
    fn get_pkey_columns() -> Vec<Self::Column> {
        vec![Column::Id]
    }
}

// ---------- helpers ----------

#[derive(thiserror::Error, Debug)]
pub enum LiveErr {
    #[error(transparent)]
    Db(#[from] sea_orm::DbErr),
}

async fn connect_and_prepare() -> DatabaseConnection {
    let conn = Database::connect(&db_url())
        .await
        .expect("connect postgres");

    // Create table if not exists
    let schema = Schema::new(DatabaseBackend::Postgres);
    let stmt = schema.create_table_from_entity(Entity);
    conn.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        stmt.to_string(PostgresQueryBuilder),
    ))
    .await
    .ok(); // ignore if exists

    // Truncate between tests
    conn.execute(Statement::from_string(
        DatabaseBackend::Postgres,
        "TRUNCATE TABLE items_filter RESTART IDENTITY".to_string(),
    ))
    .await
    .expect("truncate items_filter");

    conn
}

fn source(
    lock_for_update: bool,
    conn: DatabaseConnection,
) -> CrudablePostgresSource<Entity, (), LiveErr> {
    CrudablePostgresSource::new(conn, lock_for_update)
}

async fn seed(conn: &DatabaseConnection) {
    let now = chrono::NaiveDate::from_ymd_opt(2024, 12, 31)
        .unwrap()
        .and_hms_milli_opt(23, 59, 59, 0)
        .unwrap();

    let rows = vec![
        Model {
            id: 1,
            mono: 100,
            i: 10,
            s: "alpha".into(),
            uid: Uuid::new_v4(),
            dt: now,
            js: json!({"k": 1}),
            ia: Some(vec![1, 2]),
        },
        Model {
            id: 2,
            mono: 200,
            i: 20,
            s: "beta".into(),
            uid: Uuid::new_v4(),
            dt: now,
            js: json!({"k": 2}),
            ia: Some(vec![2, 3]),
        },
        Model {
            id: 3,
            mono: 300,
            i: 30,
            s: "beta".into(),
            uid: Uuid::new_v4(),
            dt: now,
            js: json!({"k": 3}),
            ia: None,
        },
        Model {
            id: 4,
            mono: 400,
            i: 40,
            s: "gamma".into(),
            uid: Uuid::new_v4(),
            dt: now,
            js: json!([1, 2, 3]),
            ia: Some(vec![]),
        },
        Model {
            id: 5,
            mono: 500,
            i: 100,
            s: "delta".into(),
            uid: Uuid::new_v4(),
            dt: now,
            js: json!("x"),
            ia: None,
        },
    ];

    let ams: Vec<_> = rows.into_iter().map(|m| m.into_active_model()).collect();
    Entity::insert_many(ams)
        .exec(conn)
        .await
        .expect("seed insert");
}

fn params(
    filters: Vec<CrudingListFilter<Column>>,
    sorts: Vec<(Column, CrudingListSortOrder)>,
    page: u32,
    size: u32,
) -> CrudingListParams<Column> {
    CrudingListParams {
        filters,
        sorts: sorts
            .into_iter()
            .map(|(column, order)| cruding_core::list::CrudingListSort { column, order })
            .collect(),
        pagination: CrudingListPagination { page, size },
    }
}

// ---------- tests ----------

#[tokio::test]
#[serial]
async fn eq_and_neq_filters() {
    let conn = connect_and_prepare().await;
    seed(&conn).await;
    let src = source(false, conn);
    let handle = src.new_source_handle();

    // Eq on integer: i == 20 -> id 2
    let p = params(
        vec![CrudingListFilter {
            column: Column::I,
            op: Op::Eq(json!(20)),
        }],
        vec![],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle.clone())
        .await
        .unwrap();
    assert_eq!(ids, vec![2]);

    // Neq on text: s != "beta" -> ids [1,4,5] (order unspecified without sort; we’ll sort asc)
    let p = params(
        vec![CrudingListFilter {
            column: Column::S,
            op: Op::Neq(json!("beta")),
        }],
        vec![(Column::Id, CrudingListSortOrder::Asc)],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle.clone())
        .await
        .unwrap();
    assert_eq!(ids, vec![1, 4, 5]);
}

#[tokio::test]
#[serial]
async fn comparison_filters_gt_ge_lt_le() {
    let conn = connect_and_prepare().await;
    seed(&conn).await;
    let src = source(false, conn);
    let handle = src.new_source_handle();

    // Gt: i > 30 -> ids [4,5]
    let p = params(
        vec![CrudingListFilter {
            column: Column::I,
            op: Op::Gt(json!(30)),
        }],
        vec![(Column::Id, CrudingListSortOrder::Asc)],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle.clone())
        .await
        .unwrap();
    assert_eq!(ids, vec![4, 5]);

    // Ge: i >= 30 -> [3,4,5]
    let p = params(
        vec![CrudingListFilter {
            column: Column::I,
            op: Op::Ge(json!(30)),
        }],
        vec![(Column::Id, CrudingListSortOrder::Asc)],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle.clone())
        .await
        .unwrap();
    assert_eq!(ids, vec![3, 4, 5]);

    // Lt: i < 20 -> [1]
    let p = params(
        vec![CrudingListFilter {
            column: Column::I,
            op: Op::Lt(json!(20)),
        }],
        vec![(Column::Id, CrudingListSortOrder::Asc)],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle.clone())
        .await
        .unwrap();
    assert_eq!(ids, vec![1]);

    // Le: i <= 20 -> [1,2]
    let p = params(
        vec![CrudingListFilter {
            column: Column::I,
            op: Op::Le(json!(20)),
        }],
        vec![(Column::Id, CrudingListSortOrder::Asc)],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle)
        .await
        .unwrap();
    assert_eq!(ids, vec![1, 2]);
}

#[tokio::test]
#[serial]
async fn in_and_notin_filters_uuid_and_text() {
    let conn = connect_and_prepare().await;
    seed(&conn).await;
    let src = source(false, conn.clone());
    let handle = src.new_source_handle();

    // Fetch all to learn the UUIDs
    let all: Vec<Model> = Entity::find()
        .order_by_asc(Column::Id)
        .all(&conn)
        .await
        .unwrap();
    let uid_pick = [all[1].uid, all[3].uid]; // ids 2 and 4

    // In: uid IN [uid2, uid4] -> [2,4]
    let p = params(
        vec![CrudingListFilter {
            column: Column::Uid,
            op: Op::In(vec![
                json!(uid_pick[0].to_string()),
                json!(uid_pick[1].to_string()),
            ]),
        }],
        vec![(Column::Id, CrudingListSortOrder::Asc)],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle.clone())
        .await
        .unwrap();
    assert_eq!(ids, vec![2, 4]);

    // NotIn on text: s NOT IN ("beta","gamma") -> [1,5]
    let p = params(
        vec![CrudingListFilter {
            column: Column::S,
            op: Op::NotIn(vec![json!("beta"), json!("gamma")]),
        }],
        vec![(Column::Id, CrudingListSortOrder::Asc)],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle)
        .await
        .unwrap();
    assert_eq!(ids, vec![1, 5]);
}

#[tokio::test]
#[serial]
async fn combined_filters_and_sorting() {
    let conn = connect_and_prepare().await;
    seed(&conn).await;
    let src = source(false, conn);
    let handle = src.new_source_handle();

    // WHERE s = 'beta' AND i >= 20 ORDER BY i DESC -> ids [3,2]
    let p = params(
        vec![
            CrudingListFilter {
                column: Column::S,
                op: Op::Eq(json!("beta")),
            },
            CrudingListFilter {
                column: Column::I,
                op: Op::Ge(json!(20)),
            },
        ],
        vec![(Column::I, CrudingListSortOrder::Desc)],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle)
        .await
        .unwrap();
    assert_eq!(ids, vec![3, 2]);
}

#[tokio::test]
#[serial]
async fn pagination_with_sort() {
    let conn = connect_and_prepare().await;
    seed(&conn).await;
    let src = source(false, conn);
    let handle = src.new_source_handle();

    // ORDER BY id ASC, page size 2:
    // page 0 -> [1,2], page 1 -> [3,4], page 2 -> [5]
    let common_filters = vec![]; // no filter
    let sort = vec![(Column::Id, CrudingListSortOrder::Asc)];

    let p0 = params(common_filters.clone(), sort.clone(), 0, 2);
    let p1 = params(common_filters.clone(), sort.clone(), 1, 2);
    let p2 = params(common_filters, sort, 2, 2);

    let ids0 = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p0, handle.clone())
        .await
        .unwrap();
    let ids1 = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p1, handle.clone())
        .await
        .unwrap();
    let ids2 = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p2, handle)
        .await
        .unwrap();

    assert_eq!(ids0, vec![1, 2]);
    assert_eq!(ids1, vec![3, 4]);
    assert_eq!(ids2, vec![5]);
}

#[tokio::test]
#[serial]
async fn read_list_to_ids_inside_borrowed_tx() {
    let conn = connect_and_prepare().await;
    seed(&conn).await;
    let src = source(false, conn.clone());

    let tx = conn.begin().await.unwrap();
    let handle = PostgresCrudableConnection::new(PostgresCrudableConnectionInner::BorrowedTransaction(Arc::new(tx)));
    let p = params(
        vec![CrudingListFilter {
            column: Column::I,
            op: Op::Gt(json!(15)),
        }],
        vec![(Column::Id, CrudingListSortOrder::Asc)],
        0,
        50,
    );
    let ids = CrudableSourceListExt::<Model, Column>::read_list_to_ids(&src, p, handle.clone())
        .await
        .unwrap();
    assert_eq!(ids, vec![2, 3, 4, 5]);

    // ensure handle remains borrowed
    match &*handle.get_conn().read().await {
        PostgresCrudableConnectionInner::BorrowedTransaction(_) => {}
        _ => panic!("expected borrowed transaction"),
    }
}

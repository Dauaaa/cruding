use std::{collections::HashMap, sync::Arc};

use crate::{AppState, tags};
use async_trait::async_trait;
use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use cruding::{
    handler::{CrudableHandler, CrudableHandlerImpl, CrudableHandlerListExt, MaybeArc},
    list::{CrudingListSort, CrudingListSortOrder},
    moka,
    pg_source::{
        CrudablePostgresSource, PostgresCrudableConnection, PostgresCrudableConnectionInner,
    },
};
use sea_orm::{IntoActiveModel, JoinType, QuerySelect, TransactionTrait, sea_query::OnConflict};
use tokio::try_join;

use super::*;

// tags_counter is an internal table but would still be good being setup as a crudable trait
pub mod tags_counter {
    use chrono::{DateTime, Utc};
    use cruding::{Crudable, axum_api::types::CrudableAxum, pg_source::PostgresCrudableTable};
    use sea_orm::{DeriveEntityModel, prelude::*};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, DeriveEntityModel)]
    #[sea_orm(table_name = "tags_counter")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        tag: String,
        total: i64,
        creation_time: DateTime<Utc>,
        update_time: DateTime<Utc>,
    }

    impl ActiveModelBehavior for ActiveModel {}

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    // domain impls

    impl Model {
        pub fn new(tag: String, total: i64, now: DateTime<Utc>) -> Self {
            Self {
                tag,
                total,
                creation_time: now,
                update_time: now,
            }
        }

        pub fn tag(&self) -> &String {
            &self.tag
        }

        pub fn count(&self) -> i64 {
            self.total
        }

        pub fn set_count(&mut self, new_count: i64, now: DateTime<Utc>) {
            self.total = new_count;
            self.update_time = now;
        }
    }

    // start of cruding stuff

    // Need to implement this for cruding stuff, rust should provide this implementation as a code
    // action
    impl PartialEq for Column {
        fn eq(&self, other: &Self) -> bool {
            core::mem::discriminant(self) == core::mem::discriminant(other)
        }
    }

    impl Crudable for Model {
        // This model has a composite primary key. It's important that the order is maintained!
        type Pkey = String;
        type MonoField = DateTime<Utc>;

        fn pkey(&self) -> Self::Pkey {
            self.tag.clone()
        }

        fn mono_field(&self) -> Self::MonoField {
            self.update_time
        }
    }

    /// Helper to make queries about todo
    #[derive(Debug, Serialize, Deserialize)]
    pub struct TagIdHelper {
        tag: String,
    }

    impl From<TagIdHelper> for String {
        fn from(value: TagIdHelper) -> Self {
            value.tag
        }
    }

    impl CrudableAxum for Model {
        type PkeyDe = TagIdHelper;
    }

    impl PostgresCrudableTable for Entity {
        fn get_pkey_filter(
            keys: &[<Self::Model as Crudable>::Pkey],
        ) -> impl sea_orm::sea_query::IntoCondition {
            Column::Tag.is_in(keys)
        }

        fn get_pkey_columns() -> Vec<Self::Column> {
            vec![Column::Tag]
        }
    }
}

type TagsCounterCache = moka::future::Cache<
    <tags_counter::Model as Crudable>::Pkey,
    Arc<arc_swap::ArcSwap<tags_counter::Model>>,
>;
type TagsCounterPostgresSource = CrudablePostgresSource<tags_counter::Entity, (), DbErr>;
pub type TagsCounterHandler = CrudableHandlerImpl<
    tags_counter::Model,
    TagsCounterCache,
    TagsCounterPostgresSource,
    (),
    DbErr,
    // If you add the column here (which should already implement all necessary traits if generated
    // from sea_orm) you get the listing API for free
    tags_counter::Column,
>;

pub struct PostgresTagsRepoImpl;

#[async_trait]
pub trait TagsRepo {
    /// Return a list ordered descending by how many times a tag is used
    async fn like_search_tags(
        &self,
        conn: PostgresCrudableConnection,
        needle: &str,
        page: u64,
        page_size: u64,
    ) -> Result<Vec<String>, sea_orm::DbErr>;
    /// Updates up to n counters, ordering by
    async fn update_counter(
        &self,
        tags_counter_handler: &TagsCounterHandler,
        conn_handle: PostgresCrudableConnection,
        update_at_least: u32,
    ) -> Result<(), DbErr>;
    /// Creates counters if they don't exist
    async fn declare_counters(
        &self,
        conn: PostgresCrudableConnection,
        input: Vec<tags_counter::Model>,
    ) -> Result<(), DbErr>;
}

#[async_trait]
impl TagsRepo for PostgresTagsRepoImpl {
    /// Return a list ordered descending by how many times a tag is used
    async fn like_search_tags(
        &self,
        conn: PostgresCrudableConnection,
        needle: &str,
        page: u64,
        page_size: u64,
    ) -> Result<Vec<String>, sea_orm::DbErr> {
        let q = Entity::find()
            .select_only()
            .column(Column::Tag)
            // SAFETY: sea_orm escapes these
            .filter(Column::Tag.like(format!("%{needle}%")))
            .group_by(Column::Tag)
            .into_tuple::<String>();

        let conn = conn.get_conn().read().await;
        match &*conn {
            PostgresCrudableConnectionInner::Connection(c) => {
                q.paginate(c, page_size).fetch_page(page).await
            }
            PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => {
                q.paginate(tx.as_ref(), page_size).fetch_page(page).await
            }
            PostgresCrudableConnectionInner::BorrowedTransaction(tx) => {
                q.paginate(tx.as_ref(), page_size).fetch_page(page).await
            }
        }
    }

    async fn declare_counters(
        &self,
        conn: PostgresCrudableConnection,
        input: Vec<tags_counter::Model>,
    ) -> Result<(), DbErr> {
        let q = tags_counter::Entity::insert_many(input.into_iter().map(|x| x.into_active_model()))
            .on_conflict(
                OnConflict::column(tags_counter::Column::Tag)
                    .do_nothing()
                    .to_owned(),
            )
            .do_nothing();

        let conn = conn.get_conn().read().await;
        match &*conn {
            PostgresCrudableConnectionInner::Connection(c) => q.exec(c).await?,
            PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => q.exec(tx.as_ref()).await?,
            PostgresCrudableConnectionInner::BorrowedTransaction(tx) => q.exec(tx.as_ref()).await?,
        };

        Ok(())
    }

    /// Updates up to n counters, ordering by update_time, creates missing counters and deletes
    /// zeroed counters
    async fn update_counter(
        &self,
        tags_counter_handler: &TagsCounterHandler,
        conn: PostgresCrudableConnection,
        update_at_least: u32,
    ) -> Result<(), DbErr> {
        // SUGGESTION: use an advisory lock for these types of workloads
        let counters = tags_counter_handler
            .read_list(
                cruding::list::CrudingListParams {
                    filters: vec![],
                    sorts: vec![CrudingListSort {
                        column: tags_counter::Column::UpdateTime,
                        order: CrudingListSortOrder::Asc,
                    }],
                    pagination: cruding::list::CrudingListPagination {
                        page: 0,
                        size: update_at_least,
                    },
                },
                (),
                conn.clone(),
            )
            .await?;

        tracing::debug!("Updating total of {} tag counters", counters.len());

        let mut counters = counters
            .into_iter()
            .map(|count| {
                let count = MaybeArc::take_or_clone(count);

                (count.tag().clone(), count)
            })
            .collect::<HashMap<_, _>>();
        let counter_tag_names = counters.keys();

        let query_need_update = Entity::find()
            .select_only()
            .column(Column::Tag)
            .column_as(Expr::col(Column::Tag).count(), "total")
            .filter(Column::Tag.is_in(counter_tag_names))
            .group_by(Column::Tag)
            .into_tuple::<(String, i64)>();

        let query_need_create = Entity::find()
            .join(JoinType::LeftJoin, tags::Relation::TagsCounter.def())
            .filter(Expr::col((tags_counter::Entity, tags_counter::Column::Tag)).is_null())
            .select_only()
            .column(tags::Column::Tag)
            .column_as(tags::Column::Tag.count(), "total")
            .group_by(tags::Column::Tag)
            .into_tuple::<(String, i64)>();

        let (need_create, counts) = {
            let conn = conn.get_conn().read().await;

            let get_need_create_fut = async {
                match &*conn {
                    PostgresCrudableConnectionInner::Connection(c) => {
                        query_need_create.all(c).await
                    }
                    PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => {
                        query_need_create.all(tx.as_ref()).await
                    }
                    PostgresCrudableConnectionInner::BorrowedTransaction(tx) => {
                        query_need_create.all(tx.as_ref()).await
                    }
                }
            };

            let get_counts_fut = async {
                match &*conn {
                    PostgresCrudableConnectionInner::Connection(c) => {
                        query_need_update.all(c).await
                    }
                    PostgresCrudableConnectionInner::OwnedTransaction(_, tx) => {
                        query_need_update.all(tx.as_ref()).await
                    }
                    PostgresCrudableConnectionInner::BorrowedTransaction(tx) => {
                        query_need_update.all(tx.as_ref()).await
                    }
                }
            };

            try_join!(get_need_create_fut, get_counts_fut)?
        };

        tracing::debug!("Need to create total of {} tag counters", need_create.len());

        let now = Utc::now();

        let need_create = need_create
            .into_iter()
            .map(|(tag, total)| tags_counter::Model::new(tag, total, now))
            .collect::<Vec<_>>();
        let mut to_delete = vec![];
        for count in counts {
            if count.1 == 0 {
                if let Some(counter) = counters.remove(&count.0) {
                    to_delete.push(counter.tag().clone());
                }
            } else if let Some(counter) = counters.get_mut(&count.0) {
                counter.set_count(count.1, now);
            }
        }

        if !need_create.is_empty() {
            tags_counter_handler
                .create(need_create, (), conn.clone())
                .await?;
        }
        tags_counter_handler
            .update(counters.into_values().collect(), (), conn.clone())
            .await?;
        tags_counter_handler
            .delete(to_delete, (), conn.clone())
            .await?;

        Ok(())
    }
}

pub fn build_tags_counter_handler(
    cache: TagsCounterCache,
    source: TagsCounterPostgresSource,
) -> TagsCounterHandler {
    TagsCounterHandler::new(cache, source)
}

/// Axum handler for updating counters
pub async fn update_counters(state: State<AppState>) -> (StatusCode, String) {
    if let Err(err) = async move {
        state
            .tags_repo
            .update_counter(
                state.tags_counter_handler.as_ref(),
                PostgresCrudableConnection::new(PostgresCrudableConnectionInner::OwnedTransaction(
                    state.db_conn.clone(),
                    Arc::new(state.db_conn.begin().await?),
                )),
                50,
            )
            .await
    }
    .await
    {
        (StatusCode::INTERNAL_SERVER_ERROR, err.to_string())
    } else {
        (StatusCode::OK, "updated".to_string())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TagLikeFilterQueryString {
    tag: String,
    page: u64,
    size: u64,
}

/// Axum handler for filtering tags using LIKE %{}%
pub async fn tag_like_filter(
    state: State<AppState>,
    Query(filter): Query<TagLikeFilterQueryString>,
) -> Response {
    match state
        .tags_repo
        .like_search_tags(
            PostgresCrudableConnection::new(PostgresCrudableConnectionInner::Connection(
                state.db_conn.clone(),
            )),
            &filter.tag,
            filter.page,
            filter.size,
        )
        .await
    {
        Ok(tags) => (StatusCode::OK, Json(tags)).into_response(),
        Err(err) => (StatusCode::INTERNAL_SERVER_ERROR, err.to_string()).into_response(),
    }
}

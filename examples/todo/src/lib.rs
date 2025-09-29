use std::{collections::HashMap, sync::Arc};

use axum::{
    Router,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use chrono::Utc;
use cruding::{
    Crudable, UpdateComparingParams,
    axum_api::router::CrudRouter,
    handler::CrudableHandlerImpl,
    hook::make_crudable_hook,
    moka,
    pg_source::{CrudablePostgresSource, PostgresCrudableConnection},
};
use sea_orm::DatabaseConnection;

use crate::tags::{
    PostgresTagsRepoImpl, TagsCounterHandler, TagsRepo, build_tags_counter_handler,
    tag_like_filter, update_counters,
};

pub mod todo;

pub mod tags;

/// A very bad error implementation bcs I'm lazy
#[derive(Debug)]
pub struct ApiError(Box<dyn std::error::Error + Send + Sync>);
impl From<sea_orm::DbErr> for ApiError {
    fn from(value: sea_orm::DbErr) -> Self {
        Self(value.into())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}
impl std::fmt::Display for ApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for ApiError {}

type AppCtx = AppState;
type AxumCtx = ();
// AxumCtx needs to implement IntoRequestParts, AppCtx needs to be instantiated by you handler
// implementation.
type FullCtx = Arc<(AxumCtx, AppCtx)>;

type TodoCache =
    moka::future::Cache<<todo::Model as Crudable>::Pkey, Arc<arc_swap::ArcSwap<todo::Model>>>;
type TodoPostgresSource = CrudablePostgresSource<todo::Entity, FullCtx, ApiError>;
pub type TodoHandler = CrudableHandlerImpl<
    todo::Model,
    TodoCache,
    TodoPostgresSource,
    FullCtx,
    ApiError,
    // If you add the column here (which should already implement all necessary traits if generated
    // from sea_orm) you get the listing API for free
    todo::Column,
>;

type TagsCache =
    moka::future::Cache<<tags::Model as Crudable>::Pkey, Arc<arc_swap::ArcSwap<tags::Model>>>;
type TagsPostgresSource = CrudablePostgresSource<tags::Entity, FullCtx, ApiError>;
pub type TagsHandler = CrudableHandlerImpl<
    tags::Model,
    TagsCache,
    TagsPostgresSource,
    FullCtx,
    ApiError,
    tags::Column,
>;

#[cruding_macros::cruding_axum_state(
    source_handle = PostgresCrudableConnection,
    axum_ctx = AxumCtx,
    inner_ctx = AppCtx,
    error = ApiError,
    inner_ctx_method_name = inner_ctx_impl,
    new_source_handle_method_name = new_source_handle_impl,
    listing = true,
)]
#[derive(Clone)]
pub struct AppState {
    #[crudable_handler(crud_name = "/todo", crudable = todo::Model, column = todo::Column)]
    pub todo_handler: Arc<TodoHandler>,
    #[crudable_handler(crud_name = "/tags", crudable = tags::Model, column = tags::Column)]
    pub tags_handler: Arc<TagsHandler>,
    pub tags_counter_handler: Arc<TagsCounterHandler>,
    pub tags_repo: Arc<dyn TagsRepo + Send + Sync>,
    pub db_conn: DatabaseConnection,
}

impl AppState {
    fn new_source_handle_impl(&self) -> PostgresCrudableConnection {
        PostgresCrudableConnection::new_from_conn(self.db_conn.clone())
    }

    fn inner_ctx_impl(&self) -> AppCtx {
        self.clone()
    }
}

/// Adds the necessary hooks to the todo handler
fn build_todo_handler(cache: TodoCache, source: TodoPostgresSource) -> TodoHandler {
    TodoHandler::new(cache, source)
        // this hook will initialize the todo struct
        .install_before_create(make_crudable_hook(
            |_handler, mut todos: Vec<todo::Model>, _ctx, _| {
                Box::pin(async move {
                    let now = Utc::now();
                    for todo in &mut todos {
                        todo.initialize(now);
                    }
                    Ok(todos) as Result<_, ApiError>
                })
            },
        ))
        // this hook will call the update function on the todo::Model
        .install_update_comparing(make_crudable_hook(
            |_handler,
             UpdateComparingParams {
                 current,
                 update_payload,
             }: UpdateComparingParams<todo::Model>,
             _ctx,
             _| {
                Box::pin(async move {
                    // the error here shouldn't really happen because current and update_payload
                    // should have the same entries (from the cruding impl)
                    #[derive(Debug)]
                    struct ShouldntHappen;
                    impl std::fmt::Display for ShouldntHappen {
                        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                            write!(f, "{self:?}")
                        }
                    }
                    impl std::error::Error for ShouldntHappen {}

                    let now = Utc::now();
                    let mut update_map = update_payload
                        .into_iter()
                        .map(|todo| (todo.pkey(), todo))
                        .collect::<HashMap<_, _>>();

                    for old in current {
                        update_map
                            .get_mut(&old.pkey())
                            .ok_or_else(|| ApiError(Box::new(ShouldntHappen)))?
                            .update_from_current(&old, now);
                    }

                    Ok(update_map.into_values().collect())
                })
            },
        ))
}

fn build_tags_handler(cache: TagsCache, source: TagsPostgresSource) -> TagsHandler {
    TagsHandler::new(cache, source)
        // this hook will initialize the todo struct
        .install_before_create(make_crudable_hook(
            |_handler, mut todos: Vec<tags::Model>, ctx: FullCtx, source_handle| {
                Box::pin(async move {
                    let now = Utc::now();

                    ctx.1
                        .tags_repo
                        .declare_counters(
                            source_handle,
                            todos
                                .iter()
                                .map(|tag| {
                                    tags::tags_counter::Model::new(tag.tag().clone(), 1, now)
                                })
                                .collect(),
                        )
                        .await?;

                    for todo in &mut todos {
                        todo.initialize(now);
                    }
                    Ok(todos) as Result<_, ApiError>
                })
            },
        ))
        // this hook will call the update function on the todo::Model
        .install_update_comparing(make_crudable_hook(|_handler, _, _ctx, _| {
            Box::pin(async move {
                #[derive(Debug)]
                struct DisallowUpdate;
                impl std::fmt::Display for DisallowUpdate {
                    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                        write!(f, "Updating a tag is not allowed")
                    }
                }
                impl std::error::Error for DisallowUpdate {}

                Err(ApiError(Box::new(DisallowUpdate)))
            })
        }))
}

impl AppState {
    pub fn new(db_conn: DatabaseConnection) -> Self {
        let todo_cache = moka::future::Cache::builder()
            .name("todo")
            .max_capacity(1337)
            .build();
        let tags_cache = moka::future::Cache::builder()
            .name("tags")
            .max_capacity(6969)
            .build();
        let tags_counter_cache = moka::future::Cache::builder()
            .name("tags-counter")
            .max_capacity(4242)
            .build();
        let todo_source = CrudablePostgresSource::new(db_conn.clone(), true);
        let tags_source = CrudablePostgresSource::new(db_conn.clone(), true);
        let tags_counter_source = CrudablePostgresSource::new(db_conn.clone(), true);

        AppState {
            todo_handler: Arc::new(build_todo_handler(todo_cache, todo_source)),
            tags_handler: Arc::new(build_tags_handler(tags_cache, tags_source)),
            tags_counter_handler: Arc::new(build_tags_counter_handler(
                tags_counter_cache,
                tags_counter_source,
            )),
            tags_repo: Arc::new(PostgresTagsRepoImpl),
            db_conn,
        }
    }

    pub fn todo_router() -> Router<Self> {
        CrudRouter::nested_with_list::<todo::Model, _>(None)
    }

    pub fn tags_router() -> Router<Self> {
        CrudRouter::nested_with_list::<tags::Model, _>(Some(
            Router::new()
                .route("/search", get(tag_like_filter))
                .route("/job/update-counters", post(update_counters)),
        ))
    }
}

use std::{collections::HashMap, sync::Arc};

use axum::{Router, http::StatusCode, response::IntoResponse};
use chrono::Utc;
use cruding::{
    Crudable, UpdateComparingParams,
    axum_api::{
        router::CrudRouter,
        state::{CrudableAxumState, CrudableAxumStateListExt},
    },
    handler::{CrudableHandlerGetter, CrudableHandlerGetterListExt, CrudableHandlerImpl},
    hook::make_crudable_hook,
    moka,
    pg_source::{CrudablePostgresSource, PostgresCrudableConnection},
};
use sea_orm::DatabaseConnection;

pub mod todo {
    use chrono::{DateTime, Utc};
    use cruding::{Crudable, axum_api::types::CrudableAxum, pg_source::PostgresCrudableTable};
    use sea_orm::{DeriveEntityModel, prelude::*};
    use serde::{Deserialize, Serialize};

    // normal sea_orm stuff
    #[derive(Debug, Clone, Serialize, Deserialize, DeriveEntityModel)]
    #[sea_orm(table_name = "todos")]
    pub struct Model {
        // server side fields should all have defaults and are basically ignored when deserialized
        // from a client. You know the "directionality" of the struct depending on the hook.
        //
        // For example, before_create should reset all these fields while update_comparing should
        // set these values to the current/update mono
        #[sea_orm(primary_key)]
        #[serde(default)]
        id_1: Uuid,
        #[sea_orm(primary_key)]
        #[serde(default)]
        id_2: u64,
        #[serde(default)]
        creation_time: DateTime<Utc>,
        #[serde(default)]
        update_time: DateTime<Utc>,
        #[serde(default)]
        done_time: Option<DateTime<Utc>>,

        // client fields
        pub name: String,
        pub description: String,
        pub status: TodoStatus,
    }

    #[derive(
        Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, EnumIter, DeriveActiveEnum,
    )]
    #[sea_orm(rs_type = "String", db_type = "TinyInteger", enum_name = "todo_status")]
    pub enum TodoStatus {
        #[sea_orm(string_value = "todo")]
        Todo,
        #[sea_orm(string_value = "in-progress")]
        InProgress,
        #[sea_orm(string_value = "done")]
        Done,
    }

    impl ActiveModelBehavior for ActiveModel {}

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    // domain implementation

    impl Model {
        pub fn initialize(&mut self, now: DateTime<Utc>) {
            self.id_1 = Uuid::new_v4();
            self.id_2 = self.id_1.as_u64_pair().0;
            self.creation_time = now;
            self.update_time = now;
        }

        pub fn update_from_current(&mut self, current: &Self, now: DateTime<Utc>) {
            assert_eq!(self.id_1, current.id_1);
            assert_eq!(self.id_2, current.id_2);

            // persist fixed fields
            self.creation_time = current.creation_time;

            // apply business rules
            match (current.is_done(), self.is_done()) {
                (true, false) => self.done_time = Some(now),
                (false, true) => self.done_time = None,
                (true, true) | (false, false) => {}
            }

            // update mono field
            self.update_time = now;
        }

        pub fn is_done(&self) -> bool {
            matches!(self.status, TodoStatus::Done)
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
        type Pkey = (Uuid, u64);
        type MonoField = DateTime<Utc>;

        fn pkey(&self) -> Self::Pkey {
            (self.id_1, self.id_2)
        }

        fn mono_field(&self) -> Self::MonoField {
            self.update_time
        }
    }

    /// Helper to make queries about todo
    #[derive(Debug, Serialize, Deserialize)]
    pub struct TodoIdHelper {
        id_1: Uuid,
        id_2: u64,
    }

    impl From<TodoIdHelper> for (Uuid, u64) {
        fn from(value: TodoIdHelper) -> Self {
            (value.id_1, value.id_2)
        }
    }

    impl CrudableAxum for Model {
        type PkeyDe = TodoIdHelper;
    }

    impl PostgresCrudableTable for Entity {
        fn get_pkey_filter(
            keys: &[<Self::Model as Crudable>::Pkey],
        ) -> impl sea_orm::sea_query::IntoCondition {
            Expr::tuple([Expr::column(Column::Id1), Expr::column(Column::Id2)]).is_in(
                keys.iter()
                    .map(|ids| Expr::tuple([Expr::value(ids.0), Expr::value(ids.1)])),
            )
        }

        fn get_pkey_columns() -> Vec<Self::Column> {
            vec![Column::Id1, Column::Id2]
        }
    }
}

pub mod tags {
    use chrono::{DateTime, Utc};
    use cruding::{Crudable, axum_api::types::CrudableAxum, pg_source::PostgresCrudableTable};
    use sea_orm::{ActiveModelBehavior, DeriveEntityModel, prelude::*};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Serialize, Deserialize, DeriveEntityModel)]
    #[sea_orm(table_name = "tags")]
    pub struct Model {
        // server side fields
        #[serde(default)]
        creation_time: DateTime<Utc>,
        #[serde(default)]
        update_time: DateTime<Utc>,

        // client fields
        #[sea_orm(primary_key)]
        tag: String,
        #[sea_orm(primary_key)]
        entity_id: String,
        #[sea_orm(primary_key)]
        entity_type: String,
    }

    impl ActiveModelBehavior for ActiveModel {}

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    // domain implementation

    impl Model {
        pub fn initialize(&mut self, now: DateTime<Utc>) {
            self.creation_time = now;
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
        type Pkey = (String, String, String);
        type MonoField = DateTime<Utc>;

        fn pkey(&self) -> Self::Pkey {
            (
                self.tag.clone(),
                self.entity_id.clone(),
                self.entity_type.clone(),
            )
        }

        fn mono_field(&self) -> Self::MonoField {
            self.update_time
        }
    }

    /// Helper to make queries about todo
    #[derive(Debug, Serialize, Deserialize)]
    pub struct TagIdHelper {
        tag: String,
        entity_id: String,
        entity_type: String,
    }

    impl From<TagIdHelper> for (String, String, String) {
        fn from(value: TagIdHelper) -> Self {
            (value.tag, value.entity_id, value.entity_type)
        }
    }

    impl CrudableAxum for Model {
        type PkeyDe = TagIdHelper;
    }

    impl PostgresCrudableTable for Entity {
        fn get_pkey_filter(
            keys: &[<Self::Model as Crudable>::Pkey],
        ) -> impl sea_orm::sea_query::IntoCondition {
            Expr::tuple([
                Expr::column(Column::Tag),
                Expr::column(Column::EntityId),
                Expr::column(Column::EntityType),
            ])
            .is_in(keys.iter().map(|ids| {
                Expr::tuple([
                    Expr::value(ids.0.clone()),
                    Expr::value(ids.1.clone()),
                    Expr::value(ids.2.clone()),
                ])
            }))
        }

        fn get_pkey_columns() -> Vec<Self::Column> {
            vec![Column::Tag, Column::EntityId, Column::EntityType]
        }
    }
}

/// A very bad error implementation bcs I'm lazy
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

type AppCtx = ();
type AxumCtx = ();
type FullCtx = (AxumCtx, AppCtx);

type TodoCache =
    moka::future::Cache<<todo::Model as Crudable>::Pkey, Arc<arc_swap::ArcSwap<todo::Model>>>;
type TodoPostgresSource = CrudablePostgresSource<todo::Entity, FullCtx, ApiError>;
pub type TodoHandler = CrudableHandlerImpl<
    todo::Model,
    TodoCache,
    TodoPostgresSource,
    // AxumCtx needs to implement IntoRequestParts, AppCtx needs to be instantiated by you handler
    // implementation.
    (AxumCtx, AppCtx),
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
    (AxumCtx, AppCtx),
    ApiError,
    tags::Column,
>;

#[derive(Clone)]
pub struct AppState {
    todo_handler: Arc<TodoHandler>,
    tags_handler: Arc<TagsHandler>,
    db_conn: DatabaseConnection,
}

impl CrudableHandlerGetter<todo::Model, FullCtx, PostgresCrudableConnection, ApiError>
    for AppState
{
    fn handler(
        &self,
    ) -> &dyn cruding::handler::CrudableHandler<
        todo::Model,
        FullCtx,
        PostgresCrudableConnection,
        ApiError,
    > {
        self.todo_handler.as_ref()
    }
}

impl
    CrudableHandlerGetterListExt<
        todo::Model,
        FullCtx,
        PostgresCrudableConnection,
        ApiError,
        todo::Column,
    > for AppState
{
    fn handler_list(
        &self,
    ) -> &dyn cruding::handler::CrudableHandlerListExt<
        todo::Model,
        FullCtx,
        PostgresCrudableConnection,
        ApiError,
        todo::Column,
    > {
        self.todo_handler.as_ref()
    }
}

impl CrudableHandlerGetter<tags::Model, FullCtx, PostgresCrudableConnection, ApiError>
    for AppState
{
    fn handler(
        &self,
    ) -> &dyn cruding::handler::CrudableHandler<
        tags::Model,
        FullCtx,
        PostgresCrudableConnection,
        ApiError,
    > {
        self.tags_handler.as_ref()
    }
}

impl
    CrudableHandlerGetterListExt<
        tags::Model,
        FullCtx,
        PostgresCrudableConnection,
        ApiError,
        tags::Column,
    > for AppState
{
    fn handler_list(
        &self,
    ) -> &dyn cruding::handler::CrudableHandlerListExt<
        tags::Model,
        FullCtx,
        PostgresCrudableConnection,
        ApiError,
        tags::Column,
    > {
        self.tags_handler.as_ref()
    }
}

impl CrudableAxumState<todo::Model> for AppState {
    type AxumCtx = AxumCtx;
    type InnerCtx = AppCtx;
    type SourceHandle = PostgresCrudableConnection;
    type Error = ApiError;

    // this will be used like Router::new().nest(CRUD_NAME, ...)
    const CRUD_NAME: &'static str = "/todo";

    fn new_source_handle(&self) -> Self::SourceHandle {
        PostgresCrudableConnection::Connection(self.db_conn.clone())
    }

    fn inner_ctx(&self) -> Self::InnerCtx {}
}

impl CrudableAxumStateListExt<todo::Model> for AppState {
    type Column = todo::Column;
}

impl CrudableAxumState<tags::Model> for AppState {
    type AxumCtx = AxumCtx;
    type InnerCtx = AppCtx;
    type SourceHandle = PostgresCrudableConnection;
    type Error = ApiError;

    // this will be used like Router::new().nest(CRUD_NAME, ...)
    const CRUD_NAME: &'static str = "/tags";

    fn new_source_handle(&self) -> Self::SourceHandle {
        PostgresCrudableConnection::Connection(self.db_conn.clone())
    }

    fn inner_ctx(&self) -> Self::InnerCtx {}
}

impl CrudableAxumStateListExt<tags::Model> for AppState {
    type Column = tags::Column;
}

/// Adds the necessary hooks to the todo handler
fn build_todo_handler(cache: TodoCache, source: TodoPostgresSource) -> TodoHandler {
    TodoHandler::new(cache, source)
        // this hook will initialize the todo struct
        .install_before_create(make_crudable_hook(
            |_handler, mut todos: Vec<todo::Model>, _ctx| {
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
             _ctx| {
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
            |_handler, mut todos: Vec<tags::Model>, _ctx| {
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
        .install_update_comparing(make_crudable_hook(|_handler, _, _ctx| {
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
            .name("todo-cache")
            .max_capacity(1337)
            .build();
        let tags_cache = moka::future::Cache::builder()
            .name("todo-cache")
            .max_capacity(6969)
            .build();
        let todo_source = CrudablePostgresSource::new(db_conn.clone(), true);
        let tags_source = CrudablePostgresSource::new(db_conn.clone(), true);

        AppState {
            todo_handler: Arc::new(build_todo_handler(todo_cache, todo_source)),
            tags_handler: Arc::new(build_tags_handler(tags_cache, tags_source)),
            db_conn,
        }
    }

    pub fn todo_router(&self) -> Router<Self> {
        CrudRouter::with_list::<todo::Model, _>()
    }

    pub fn tags_router(&self) -> Router<Self> {
        CrudRouter::with_list::<tags::Model, _>()
    }
}

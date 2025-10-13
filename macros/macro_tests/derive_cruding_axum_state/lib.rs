#![allow(dead_code)]

use std::sync::Arc;

use axum::{http::StatusCode, response::IntoResponse};
use cruding::{
    handler::CrudableHandlerImpl,
    pg_source::{CrudablePostgresSource, PostgresCrudableConnection},
};
use cruding_in_mem_cache::CrudingInMemoryCache;
use sea_orm::DatabaseConnection;

mod todo {
    use chrono::{DateTime, Utc};
    use cruding_macros::crudable_seaorm;
    use sea_orm::{DeriveEntityModel, prelude::*};
    use serde::{Deserialize, Serialize};

    #[crudable_seaorm(axum)]
    #[derive(Debug, Clone, Serialize, Deserialize, DeriveEntityModel)]
    #[sea_orm(table_name = "todos")]
    pub struct Model {
        #[serde(default)]
        id_1: Uuid,
        #[sea_orm(primary_key, auto_increment = false)]
        #[serde(default)]
        id_2: String,
        #[serde(default)]
        creation_time: DateTime<Utc>,
        #[serde(default)]
        #[crudable(mono)]
        update_time: DateTime<Utc>,
        #[serde(default)]
        done_time: Option<DateTime<Utc>>,

        pub name: String,
        pub description: String,
    }

    impl ActiveModelBehavior for ActiveModel {}

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}
}

mod todo_2 {
    use chrono::{DateTime, Utc};
    use cruding_macros::crudable_seaorm;
    use sea_orm::{DeriveEntityModel, prelude::*};
    use serde::{Deserialize, Serialize};

    #[crudable_seaorm(axum)]
    #[derive(Debug, Clone, Serialize, Deserialize, DeriveEntityModel)]
    #[sea_orm(table_name = "todos_2")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        #[serde(default)]
        id_1: Uuid,
        #[sea_orm(primary_key, auto_increment = false)]
        #[serde(default)]
        id_2: String,
        #[serde(default)]
        creation_time: DateTime<Utc>,
        #[serde(default)]
        #[crudable(mono)]
        update_time: DateTime<Utc>,
        #[serde(default)]
        done_time: Option<DateTime<Utc>>,

        pub name: String,
        pub description: String,
    }

    impl ActiveModelBehavior for ActiveModel {}

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}
}

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
type FullCtx = Arc<(AxumCtx, AppCtx)>;
type TodoCache = CrudingInMemoryCache<todo::Model>;
type TodoCache2 = CrudingInMemoryCache<todo_2::Model>;
type TodoPostgresSource = CrudablePostgresSource<todo::Entity, FullCtx, ApiError>;
type TodoPostgresSource2 = CrudablePostgresSource<todo_2::Entity, FullCtx, ApiError>;
type TodoHandler = CrudableHandlerImpl<
    todo::Model,
    TodoCache,
    TodoPostgresSource,
    FullCtx,
    ApiError,
    todo::Column,
>;
type TodoHandler2 = CrudableHandlerImpl<
    todo_2::Model,
    TodoCache2,
    TodoPostgresSource2,
    FullCtx,
    ApiError,
    todo_2::Column,
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
    todo_handler: Arc<TodoHandler>,
    #[crudable_handler(crud_name = "/todo2", crudable = todo_2::Model, column = todo_2::Column)]
    todo_handler_2: Arc<TodoHandler2>,
    db_conn: DatabaseConnection,
}

impl AppState {
    fn new_source_handle_impl(&self) -> PostgresCrudableConnection {
        PostgresCrudableConnection::new_from_conn(self.db_conn.clone())
    }

    fn inner_ctx_impl(&self) -> AppCtx {
        self.clone()
    }
}

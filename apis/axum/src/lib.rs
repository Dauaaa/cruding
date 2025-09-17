use axum::{
    Json,
    extract::{FromRequestParts, Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use cruding_core::{
    Crudable,
    handler::{CrudableHandlerGetter, CrudableHandlerImpl},
};
use cruding_pg_source::{CrudablePostgresSource, PostgresCrudableConnection};
use sea_orm::prelude::*;
use serde::{Deserialize, Serialize};

pub trait CrudableAxum: Serialize + for<'de> Deserialize<'de> {}
impl<X> CrudableAxum for X where X: Serialize + for<'de> Deserialize<'de> {}
pub trait CrudableAxumState<CRUD, Ctx, SourceHandle, Error>:
    CrudableHandlerGetter<CRUD, Ctx, SourceHandle, Error> + Clone
where
    Ctx: FromRequestParts<Self>,
    CRUD: CrudableAxum,
    Error: IntoResponse,
{
}

pub fn x<CRUD: CrudableAxum>(a: impl CrudableAxumState<CRUD, (), (), ()>) {}

async fn a<CRUD, Ctx, SourceHandle, Error, S>(s: State<S>, ctx: Ctx, Json(payload): Json<CRUD>)
where
    S: CrudableHandlerGetter<CRUD, Ctx, SourceHandle, Error>,
    Ctx: FromRequestParts<S>,
    CRUD: CrudableAxum,
    Error: IntoResponse,
{
}

#[derive(Debug, Clone, Serialize, Deserialize, DeriveEntityModel)]
#[sea_orm(table_name = "example")]
pub struct Model {
    #[sea_orm(primary_key)]
    pub id: i32,
}

#[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
pub enum Relation {}

impl ActiveModelBehavior for ActiveModel {}

impl Crudable for Model {
    type Pkey = i32;

    type MonoField = i32;

    fn pkey(&self) -> Self::Pkey {
        self.id
    }

    fn mono_field(&self) -> Self::MonoField {
        self.id
    }
}

struct E;
impl IntoResponse for E {
    fn into_response(self) -> axum::response::Response {
        StatusCode::INTERNAL_SERVER_ERROR.into_response()
    }
}

type Ctx = (HeaderMap, Path<(String, String)>, Query<serde_json::Value>);
type Testler<Map> = CrudableHandlerImpl<Model, Map, CrudablePostgresSource<Entity, Ctx, E>, Ctx, E>;

// tests/get_list.rs
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use axum::{
    Router,
    body::Body,
    extract::FromRequestParts,
    http::{Request, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use cruding_core::{
    Crudable,
    handler::{
        CrudableHandler, CrudableHandlerGetter, CrudableHandlerGetterListExt,
        CrudableHandlerListExt, MaybeArc,
    },
    list::CrudingListParams,
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

use cruding_axum_api::prelude::*; // CrudRouter, CrudableAxum, CrudableAxumState, CrudableAxumStateListExt, etc.

// ---------- Test types ----------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct Thing {
    id: i32,
    mono: i64,
    data: String,
}

impl Crudable for Thing {
    type Pkey = i32;
    type MonoField = i64;
    fn pkey(&self) -> Self::Pkey {
        self.id
    }
    fn mono_field(&self) -> Self::MonoField {
        self.mono
    }
}

impl CrudableAxum for Thing {
    type PkeyDe = i32;
}

// Columns for the list API (only needed to satisfy trait bounds)
#[derive(Clone, Copy, Debug)]
enum Column {
    Id,
    Mono,
    Data,
}
impl std::str::FromStr for Column {
    type Err = &'static str;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "id" => Ok(Column::Id),
            "mono" => Ok(Column::Mono),
            "data" => Ok(Column::Data),
            _ => Err("invalid column"),
        }
    }
}

// Axum-extracted ctx (e.g. a user from headers)
#[derive(Clone, Debug, PartialEq)]
struct AxumCtx {
    user: String,
}

// Extract AxumCtx from "X-User" header
impl<S> FromRequestParts<S> for AxumCtx
where
    S: Send + Sync,
{
    type Rejection = Response;

    fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        _state: &S,
    ) -> impl Future<Output = Result<Self, Self::Rejection>> + Send {
        Box::pin(async move {
            let user = parts
                .headers
                .get("X-User")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("anonymous")
                .to_string();
            Ok(AxumCtx { user })
        })
    }
}

// Inner ctx (not extracted by Axum)
#[derive(Clone, Debug, PartialEq)]
struct InnerCtx {
    shard: u16,
}

// Our SourceHandle and Error types for tests
#[derive(Clone, Debug)]
struct SrcHandle;

#[allow(dead_code)]
#[derive(Debug)]
enum ApiError {
    Internal(&'static str),
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", self)).into_response()
    }
}

// A tiny in-memory handler that records calls
#[derive(Clone, Default)]
struct TestHandler {
    calls: Arc<Mutex<Vec<String>>>,
}

impl TestHandler {
    fn push(&self, s: impl Into<String>) {
        self.calls.lock().unwrap().push(s.into());
    }
    fn take(&self) -> Vec<String> {
        std::mem::take(&mut *self.calls.lock().unwrap())
    }
}

#[async_trait]
impl CrudableHandler<Thing, (AxumCtx, InnerCtx), SrcHandle, ApiError> for TestHandler {
    async fn create(
        &self,
        _input: Vec<Thing>,
        _ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        unreachable!("not used in this test");
    }

    async fn read(
        &self,
        _keys: Vec<i32>,
        _ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        unreachable!("not used in this test");
    }

    async fn update(
        &self,
        _input: Vec<Thing>,
        _ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        unreachable!("not used in this test");
    }

    async fn delete(
        &self,
        _keys: Vec<i32>,
        _ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<(), ApiError> {
        unreachable!("not used in this test");
    }
}

// Implement the *list* handler trait for our TestHandler
#[async_trait]
impl CrudableHandlerListExt<Thing, (AxumCtx, InnerCtx), SrcHandle, ApiError, Column>
    for TestHandler
{
    async fn read_list(
        &self,
        _params: CrudingListParams<Column>,
        ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        self.push(format!("list user={} shard={}", ctx.0.user, ctx.1.shard));
        // Return a stable, deterministic set
        let out = vec![
            Thing {
                id: 1,
                mono: 10,
                data: "a".into(),
            },
            Thing {
                id: 2,
                mono: 20,
                data: "b".into(),
            },
            Thing {
                id: 3,
                mono: 30,
                data: "c".into(),
            },
        ];
        Ok(out.into_iter().map(MaybeArc::Owned).collect())
    }
}

// State implements both Getter traits + Axum state traits
#[derive(Clone)]
struct AppState {
    handler: TestHandler,
    inner: InnerCtx,
}

impl CrudableHandlerGetter<Thing, (AxumCtx, InnerCtx), SrcHandle, ApiError> for AppState {
    fn handler(
        &self,
    ) -> &dyn cruding_core::handler::CrudableHandler<Thing, (AxumCtx, InnerCtx), SrcHandle, ApiError>
    {
        &self.handler
    }
}
impl CrudableHandlerGetterListExt<Thing, (AxumCtx, InnerCtx), SrcHandle, ApiError, Column>
    for AppState
{
    fn handler_list(
        &self,
    ) -> &dyn cruding_core::handler::CrudableHandlerListExt<
        Thing,
        (AxumCtx, InnerCtx),
        SrcHandle,
        ApiError,
        Column,
    > {
        &self.handler
    }
}

impl CrudableAxumState for AppState
where
    Thing: CrudableAxum,
{
    type CRUD = Thing;
    type AxumCtx = AxumCtx;
    type InnerCtx = InnerCtx;
    type SourceHandle = SrcHandle;
    type Error = ApiError;

    const CRUD_NAME: &'static str = "/things";

    fn new_source_handle(&self) -> SrcHandle {
        SrcHandle
    }
    fn inner_ctx(&self) -> InnerCtx {
        self.inner.clone()
    }
}

impl CrudableAxumStateListExt for AppState {
    type Column = Column;
}

fn mk_app() -> (Router<()>, AppState) {
    let state = AppState {
        handler: TestHandler::default(),
        inner: InnerCtx { shard: 7 },
    };

    // Optional plugin under same nest
    let plugin = Router::new().route("/ping", get(|| async { "pong" }));

    // Include the /list route
    let app = CrudRouter::nested_with_list::<AppState>(Some(plugin)).with_state(state.clone());
    (app, state)
}

// ---------- Tests ----------

#[tokio::test]
async fn list_returns_items_and_ctx_is_passed() {
    let (app, state) = mk_app();

    let req = Request::builder()
        .method("GET")
        .uri("/things/list") // no query params needed for this smoke test
        .header("X-User", "alice")
        .body(Body::empty())
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = resp
        .into_body()
        .into_data_stream()
        .map_ok(|x| x.to_vec())
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .concat();

    let out: Vec<Thing> = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(
        out,
        vec![
            Thing {
                id: 1,
                mono: 10,
                data: "a".into()
            },
            Thing {
                id: 2,
                mono: 20,
                data: "b".into()
            },
            Thing {
                id: 3,
                mono: 30,
                data: "c".into()
            },
        ]
    );

    // ensure the handler saw AxumCtx + InnerCtx
    let calls = state.handler.take();
    assert!(calls.iter().any(|l| l == "list user=alice shard=7"));
}

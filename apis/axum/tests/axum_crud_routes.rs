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
    handler::{CrudableHandler, CrudableHandlerGetter, MaybeArc},
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use tower::ServiceExt;

// Bring your trait code into scope
use cruding_axum_api::{CrudableAxum, CrudableAxumState, CrudableHandlerGetterAxumExt};

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
    // We'll accept plain ints as the read/delete input type
    type PkeyDe = i32;
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
        input: Vec<Thing>,
        ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        self.push(format!("create user={} shard={}", ctx.0.user, ctx.1.shard));
        Ok(input.into_iter().map(MaybeArc::Owned).collect())
    }

    async fn read(
        &self,
        keys: Vec<i32>,
        ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        self.push(format!(
            "read user={} shard={} keys={:?}",
            ctx.0.user, ctx.1.shard, keys
        ));
        // Fabricate items with same ids
        let items: Vec<Thing> = keys
            .into_iter()
            .map(|id| Thing {
                id,
                mono: 0,
                data: format!("ok-{id}"),
            })
            .collect();
        Ok(items.into_iter().map(MaybeArc::Owned).collect())
    }

    async fn update(
        &self,
        input: Vec<Thing>,
        ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        self.push(format!("update user={} shard={}", ctx.0.user, ctx.1.shard));
        // echo back
        Ok(input.into_iter().map(MaybeArc::Owned).collect())
    }

    async fn delete(
        &self,
        keys: Vec<i32>,
        ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<(), ApiError> {
        self.push(format!(
            "delete user={} shard={} keys={:?}",
            ctx.0.user, ctx.1.shard, keys
        ));
        Ok(())
    }
}

// State implements Getter + Axum state trait
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

fn mk_app() -> (Router<()>, AppState) {
    let state = AppState {
        handler: TestHandler::default(),
        inner: InnerCtx { shard: 7 },
    };

    // Optional plugin under same nest
    let plugin = Router::new().route("/ping", get(|| async { "pong" }));

    let app = state
        .handler
        .into_crud_router(Some(plugin))
        .with_state(state.clone());
    (app, state)
}

// ---------- Tests ----------

#[tokio::test]
async fn create_roundtrip_and_ctx_are_passed() {
    let (app, state) = mk_app();
    let payload = vec![
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
    ];
    let req = Request::builder()
        .method("POST")
        .uri("/things/create")
        .header("content-type", "application/json")
        .header("X-User", "alice")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
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
    assert_eq!(out, payload);

    // ctx recorded by handler
    let calls = state.handler.take();
    assert!(calls.iter().any(|l| l == "create user=alice shard=7"));
}

#[tokio::test]
async fn read_uses_pkeyde_and_returns_items() {
    let (app, state) = mk_app();
    // read/delete accept Vec<PkeyDe> = Vec<i32>
    let keys = vec![5, 9];
    let req = Request::builder()
        .method("POST")
        .uri("/things/read")
        .header("content-type", "application/json")
        .header("X-User", "bob")
        .body(Body::from(serde_json::to_vec(&keys).unwrap()))
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
    assert_eq!(out.len(), 2);
    assert_eq!(out[0].id, 5);
    assert_eq!(out[1].id, 9);

    let calls = state.handler.take();
    // ensure AxumCtx + InnerCtx made it through, and keys were converted
    assert!(calls.iter().any(|l| l.contains("read user=bob shard=7")));
    assert!(calls.iter().any(|l| l.contains("keys=[5, 9]")));
}

#[tokio::test]
async fn update_roundtrip() {
    let (app, state) = mk_app();
    let payload = vec![Thing {
        id: 3,
        mono: 99,
        data: "zzz".into(),
    }];

    let req = Request::builder()
        .method("POST")
        .uri("/things/update")
        .header("content-type", "application/json")
        .header("X-User", "carol")
        .body(Body::from(serde_json::to_vec(&payload).unwrap()))
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
    assert_eq!(out, payload);

    let calls = state.handler.take();
    assert!(calls.iter().any(|l| l == "update user=carol shard=7"));
}

#[tokio::test]
async fn delete_uses_pkeyde_and_returns_204() {
    let (app, state) = mk_app();
    let keys = vec![42];

    let req = Request::builder()
        .method("POST")
        .uri("/things/delete")
        .header("content-type", "application/json")
        .header("X-User", "dan")
        .body(Body::from(serde_json::to_vec(&keys).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NO_CONTENT);

    let calls = state.handler.take();
    assert!(
        calls
            .iter()
            .any(|l| l.contains("delete user=dan shard=7 keys=[42]"))
    );
}

#[tokio::test]
async fn plugin_routes_are_merged_under_same_nest() {
    let (app, _state) = mk_app();

    let req = Request::builder()
        .method("GET")
        .uri("/things/ping")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = resp
        .into_body()
        .into_data_stream()
        .map_ok(|x| x.to_vec())
        .try_collect::<Vec<_>>()
        .await
        .unwrap()
        .concat();
    assert_eq!(body_bytes, b"pong");
}

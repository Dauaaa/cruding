// tests/list_qs_axum.rs
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
    list::{CrudingListFilterOperators, CrudingListParams, CrudingListSortOrder},
};
use tower::ServiceExt;

use cruding_axum_api::prelude::*;

// ---------- Test model ----------

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
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

// Columns (parsed from qs)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

// Axum-extracted ctx
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

// Inner ctx + SourceHandle + Error
#[derive(Clone, Debug, PartialEq)]
struct InnerCtx {
    shard: u16,
}

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

// -------- Handler that CAPTURES the parsed params --------

#[derive(Clone, Default)]
struct TestHandler {
    last_params: Arc<Mutex<Option<CrudingListParams<Column>>>>,
}

#[async_trait]
impl CrudableHandler<Thing, (AxumCtx, InnerCtx), SrcHandle, ApiError> for TestHandler {
    async fn create(
        &self,
        _i: Vec<Thing>,
        _c: &mut (AxumCtx, InnerCtx),
        _s: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        unreachable!()
    }
    async fn read(
        &self,
        _k: Vec<i32>,
        _c: &mut (AxumCtx, InnerCtx),
        _s: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        unreachable!()
    }
    async fn update(
        &self,
        _i: Vec<Thing>,
        _c: &mut (AxumCtx, InnerCtx),
        _s: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        unreachable!()
    }
    async fn delete(
        &self,
        _k: Vec<i32>,
        _c: &mut (AxumCtx, InnerCtx),
        _s: &mut SrcHandle,
    ) -> Result<(), ApiError> {
        unreachable!()
    }
}

#[async_trait]
impl CrudableHandlerListExt<Thing, (AxumCtx, InnerCtx), SrcHandle, ApiError, Column>
    for TestHandler
{
    async fn read_list(
        &self,
        params: CrudingListParams<Column>,
        _ctx: &mut (AxumCtx, InnerCtx),
        _sh: &mut SrcHandle,
    ) -> Result<Vec<MaybeArc<Thing>>, ApiError> {
        *self.last_params.lock().unwrap() = Some(params);
        // Return something trivial; body content isn't the focus here
        let out = vec![Thing {
            id: 1,
            mono: 10,
            data: "ok".into(),
        }];
        Ok(out.into_iter().map(MaybeArc::Owned).collect())
    }
}

impl TestHandler {
    fn take_params(&self) -> Option<CrudingListParams<Column>> {
        self.last_params.lock().unwrap().take()
    }
}

// -------- State & Router --------

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
impl CrudableAxumState<Thing> for AppState
where
    Thing: CrudableAxum,
{
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
impl CrudableAxumStateListExt<Thing> for AppState {
    type Column = Column;
}

fn mk_app() -> (Router<()>, AppState) {
    let state = AppState {
        handler: TestHandler::default(),
        inner: InnerCtx { shard: 7 },
    };
    let plugin = Router::new().route("/ping", get(|| async { "pong" }));
    let app =
        CrudRouter::nested_with_list::<Thing, AppState>(Some(plugin)).with_state(state.clone());
    (app, state)
}

// ---------- Tests ----------

#[tokio::test]
async fn list_parses_filters_sorts_pagination() {
    let (app, state) = mk_app();

    // Mix filters/sorts/pagination to ensure ordering and parsing work.
    // - filter[data][=]="hi"
    // - filter[mono][>=]=20
    // - sort[id]=desc
    // - pagination[page]=2
    // - pagination[size]=50
    let qs = urlencoding::encode(
        "filter[data][=]=\"hi\"&\
        filter[mono][>=]=20&\
        sort[id]=desc&\
        pagination[page]=2&\
        pagination[size]=50",
    );

    let req = Request::builder()
        .method("GET")
        .uri(format!("/things/list?{qs}"))
        .header("X-User", "alice")
        .body(Body::empty())
        .unwrap();

    println!("{req:?}");

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let params = state.handler.take_params().expect("params captured");
    // 1) Filters
    assert_eq!(params.filters.len(), 2);
    // data == "hi"
    match &params.filters[0].op {
        CrudingListFilterOperators::Eq(v) => assert_eq!(v, &serde_json::json!("hi")),
        other => panic!("unexpected op: {:?}", other),
    }
    assert_eq!(params.filters[0].column, Column::Data);

    // mono >= 20
    match &params.filters[1].op {
        CrudingListFilterOperators::Ge(v) => assert_eq!(v, &serde_json::json!(20)),
        other => panic!("unexpected op: {:?}", other),
    }
    assert_eq!(params.filters[1].column, Column::Mono);

    // 2) Sorts (order preserved)
    assert_eq!(params.sorts.len(), 1);
    assert_eq!(params.sorts[0].column, Column::Id);
    assert!(matches!(params.sorts[0].order, CrudingListSortOrder::Desc));

    // 3) Pagination
    assert_eq!(params.pagination.page, 2);
    assert_eq!(params.pagination.size, 50);
}

#[tokio::test]
async fn list_parses_in_and_notin_arrays() {
    let (app, state) = mk_app();

    // in with array
    let url = "/things/list?filter[id][in]=[1,2,3]";
    let req = Request::builder()
        .method("GET")
        .uri(url)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let p = state.handler.take_params().unwrap();
    assert_eq!(p.filters.len(), 1);
    match &p.filters[0].op {
        CrudingListFilterOperators::In(v) => {
            assert_eq!(
                v,
                &vec![
                    serde_json::json!(1),
                    serde_json::json!(2),
                    serde_json::json!(3)
                ]
            )
        }
        other => panic!("expected In, got {:?}", other),
    }

    // !in with array
    let url = "/things/list?filter[id][!in]=[4,5]";
    let req = Request::builder()
        .method("GET")
        .uri(url)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let p = state.handler.take_params().unwrap();
    match &p.filters[0].op {
        CrudingListFilterOperators::NotIn(v) => {
            assert_eq!(v, &vec![serde_json::json!(4), serde_json::json!(5)])
        }
        other => panic!("expected NotIn, got {:?}", other),
    }
}

#[tokio::test]
async fn list_bad_in_not_array_returns_400() {
    let (app, _state) = mk_app();

    // in with non-array must fail qs parsing → 400
    let url = "/things/list?filter[id][in]=1";
    let req = Request::builder()
        .method("GET")
        .uri(url)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_bad_column_returns_400() {
    let (app, _state) = mk_app();

    // unknown column name → 400
    let url = "/things/list?filter[oops][=]=1";
    let req = Request::builder()
        .method("GET")
        .uri(url)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn list_multiple_sorts_preserve_order() {
    let (app, state) = mk_app();

    let url = "/things/list?sort[id]=asc&sort[data]=desc";
    let req = Request::builder()
        .method("GET")
        .uri(url)
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let p = state.handler.take_params().unwrap();
    assert_eq!(p.sorts.len(), 2);
    assert_eq!(p.sorts[0].column, Column::Id);
    assert!(matches!(p.sorts[0].order, CrudingListSortOrder::Asc));
    assert_eq!(p.sorts[1].column, Column::Data);
    assert!(matches!(p.sorts[1].order, CrudingListSortOrder::Desc));
}

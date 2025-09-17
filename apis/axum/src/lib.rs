use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{FromRequestParts, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
};
use cruding_core::{
    Crudable,
    handler::{CrudableHandler, CrudableHandlerGetter, MaybeArc},
};
use serde::{Deserialize, Serialize};

pub trait CrudableAxum: Crudable + Serialize + for<'de> Deserialize<'de>
where
    <Self as Crudable>::Pkey: From<<Self as CrudableAxum>::PkeyDe>,
{
    type PkeyDe: for<'de> Deserialize<'de> + Send;
}

pub trait CrudableAxumState:
    CrudableHandlerGetter<
        Self::CRUD,
        (Self::AxumCtx, Self::InnerCtx),
        Self::SourceHandle,
        Self::Error,
    > + Clone
where
    <Self::CRUD as Crudable>::Pkey: From<<Self::CRUD as CrudableAxum>::PkeyDe>,
{
    type AxumCtx: FromRequestParts<Self>;
    type InnerCtx: Send + 'static;
    type SourceHandle: Send;
    type CRUD: CrudableAxum;
    type Error: IntoResponse;

    const CRUD_NAME: &'static str;

    fn new_source_handle(&self) -> Self::SourceHandle;
    fn inner_ctx(&self) -> Self::InnerCtx;
}

fn into_owned_vec<CRUD: Clone>(v: Vec<MaybeArc<CRUD>>) -> Vec<CRUD> {
    v.into_iter()
        .map(|m| match m {
            MaybeArc::Arced(a) => (*a).clone(),
            MaybeArc::Owned(x) => x,
        })
        .collect()
}

pub trait CrudableHandlerGetterAxumExt<AxumState>:
    CrudableHandler<
        AxumState::CRUD,
        (AxumState::AxumCtx, AxumState::InnerCtx),
        AxumState::SourceHandle,
        AxumState::Error,
    >
where
    AxumState: CrudableAxumRoutes + Send + Sync + 'static,
    AxumState::AxumCtx: FromRequestParts<AxumState> + Send,
    AxumState::InnerCtx: Send + 'static,
    AxumState::CRUD: CrudableAxum,
    AxumState::Error: IntoResponse + Send,
{
    fn into_crud_router(&self, plugins: Option<Router<AxumState>>) -> Router<AxumState> {
        let mut inner_router = Router::new()
            .route("/create", post(AxumState::create_handler))
            .route("/read", post(AxumState::read_handler))
            .route("/update", post(AxumState::update_handler))
            .route("/delete", post(AxumState::delete_handler));

        if let Some(plugins) = plugins {
            inner_router = inner_router.merge(plugins);
        }

        Router::new().nest(AxumState::CRUD_NAME, inner_router)
    }
}

impl<AxumState, H> CrudableHandlerGetterAxumExt<AxumState> for H
where
    H: CrudableHandler<
            AxumState::CRUD,
            (AxumState::AxumCtx, AxumState::InnerCtx),
            AxumState::SourceHandle,
            AxumState::Error,
        >,
    AxumState: CrudableAxumRoutes + Send + Sync + 'static,
    AxumState::AxumCtx: FromRequestParts<AxumState> + Send,
    AxumState::InnerCtx: Send + 'static,
    AxumState::CRUD: CrudableAxum,
    AxumState::Error: IntoResponse + Send,
{
}

#[async_trait]
pub trait CrudableAxumRoutes
where
    Self: CrudableAxumState + Send + Sync + 'static,
    Self::AxumCtx: FromRequestParts<Self> + Send,
    Self::InnerCtx: Send + 'static,
    Self::CRUD: CrudableAxum,
    Self::Error: IntoResponse + Send,
{
    #[tracing::instrument(skip_all)]
    async fn create_handler(
        State(state): State<Self>,
        ctx: Self::AxumCtx,
        Json(items): Json<Vec<Self::CRUD>>,
    ) -> Result<impl IntoResponse, Self::Error> {
        let mut sh = state.new_source_handle();
        let mut ctx = (ctx, state.inner_ctx());

        let out = state.handler().create(items, &mut ctx, &mut sh).await?;
        Ok(Json(into_owned_vec(out)))
    }

    #[tracing::instrument(skip_all)]
    async fn read_handler(
        State(state): State<Self>,
        ctx: Self::AxumCtx,
        Json(keys): Json<Vec<<Self::CRUD as CrudableAxum>::PkeyDe>>,
    ) -> Result<impl IntoResponse, Self::Error> {
        let keys = keys.into_iter().map(Into::into).collect();
        let mut sh = state.new_source_handle();
        let mut ctx = (ctx, state.inner_ctx());

        let out = state.handler().read(keys, &mut ctx, &mut sh).await?;
        Ok(Json(into_owned_vec(out)))
    }

    #[tracing::instrument(skip_all)]
    async fn update_handler(
        State(state): State<Self>,
        ctx: Self::AxumCtx,
        Json(items): Json<Vec<Self::CRUD>>,
    ) -> Result<impl IntoResponse, Self::Error> {
        let mut sh = state.new_source_handle();
        let mut ctx = (ctx, state.inner_ctx());

        let out = state.handler().update(items, &mut ctx, &mut sh).await?;
        Ok(Json(into_owned_vec(out)))
    }

    #[tracing::instrument(skip_all)]
    async fn delete_handler(
        State(state): State<Self>,
        ctx: Self::AxumCtx,
        Json(keys): Json<Vec<<Self::CRUD as CrudableAxum>::PkeyDe>>,
    ) -> Result<impl IntoResponse, Self::Error> {
        let keys = keys.into_iter().map(Into::into).collect();
        let mut sh = state.new_source_handle();
        let mut ctx = (ctx, state.inner_ctx());

        state.handler().delete(keys, &mut ctx, &mut sh).await?;
        Ok(StatusCode::NO_CONTENT)
    }
}

impl<S> CrudableAxumRoutes for S
where
    S: CrudableAxumState + Send + Sync + 'static,
    S::CRUD: Crudable + CrudableAxum + Send + Sync + 'static,
    <S::CRUD as Crudable>::Pkey: From<<S::CRUD as CrudableAxum>::PkeyDe>,
    S::AxumCtx: FromRequestParts<S> + Send + 'static,
    S::InnerCtx: Send + 'static,
    S::SourceHandle: Send + 'static,
    S::Error: IntoResponse + Send + 'static,
{
}

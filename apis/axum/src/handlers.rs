use std::sync::Arc;

use super::state::{CrudableAxumState, CrudableAxumStateListExt, into_owned_vec};
use super::types::CrudableAxum;
use crate::extractors::{AxumListParams, VecOrSingle};
use axum::response::IntoResponse;
use axum::{Json, extract::State};

fn with_ctx<CRUD, S, R>(
    state: S,
    ax_ctx: S::AxumCtx,
    f: impl FnOnce(S, super::types::ReqCtx<S::AxumCtx, S::InnerCtx>, S::SourceHandle) -> R,
) -> R
where
    CRUD: CrudableAxum,
    CRUD::Pkey: From<CRUD::PkeyDe>,
    S: CrudableAxumState<CRUD>,
{
    let sh = state.new_source_handle();
    let ctx = Arc::new((ax_ctx, state.inner_ctx()));
    f(state, ctx, sh)
}

pub async fn create<CRUD, S>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    Json(VecOrSingle(items)): Json<VecOrSingle<CRUD>>,
) -> Result<impl IntoResponse, S::Error>
where
    CRUD: CrudableAxum,
    CRUD::Pkey: From<CRUD::PkeyDe>,
    S: CrudableAxumState<CRUD>,
    S::Error: Send,
    S::AxumCtx: Send + Sync,
    S::InnerCtx: Send + Sync,
{
    with_ctx(state, ax_ctx, |s, ctx, sh| async move {
        let out = s.handler().create(items, ctx, sh).await?;
        Ok(Json(into_owned_vec(out)))
    })
    .await
}

pub async fn read<CRUD, S>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    Json(VecOrSingle(keys)): Json<VecOrSingle<<CRUD as CrudableAxum>::PkeyDe>>,
) -> Result<impl IntoResponse, S::Error>
where
    CRUD: CrudableAxum,
    CRUD::Pkey: From<CRUD::PkeyDe>,
    S: CrudableAxumState<CRUD>,
    S::Error: Send,
    S::AxumCtx: Send + Sync,
    S::InnerCtx: Send + Sync,
{
    let keys = keys.into_iter().map(Into::into).collect();
    with_ctx(state, ax_ctx, |s, ctx, sh| async move {
        let out = s.handler().read(keys, ctx, sh).await?;
        Ok(Json(into_owned_vec(out)))
    })
    .await
}

pub async fn update<CRUD, S>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    Json(VecOrSingle(items)): Json<VecOrSingle<CRUD>>,
) -> Result<impl IntoResponse, S::Error>
where
    CRUD: CrudableAxum,
    CRUD::Pkey: From<CRUD::PkeyDe>,
    S: CrudableAxumState<CRUD>,
    S::Error: Send,
    S::AxumCtx: Send + Sync,
    S::InnerCtx: Send + Sync,
{
    with_ctx(state, ax_ctx, |s, ctx, sh| async move {
        let out = s.handler().update(items, ctx, sh).await?;
        Ok(Json(into_owned_vec(out)))
    })
    .await
}

pub async fn delete<CRUD, S>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    Json(VecOrSingle(keys)): Json<VecOrSingle<<CRUD as CrudableAxum>::PkeyDe>>,
) -> Result<impl IntoResponse, S::Error>
where
    CRUD: CrudableAxum,
    CRUD::Pkey: From<CRUD::PkeyDe>,
    S: CrudableAxumState<CRUD>,
    S::Error: Send,
    S::AxumCtx: Send + Sync,
    S::InnerCtx: Send + Sync,
{
    let keys = keys.into_iter().map(Into::into).collect();
    with_ctx(state, ax_ctx, |s, ctx, sh| async move {
        s.handler().delete(keys, ctx, sh).await?;
        Ok(axum::http::StatusCode::NO_CONTENT)
    })
    .await
}

pub async fn read_list<CRUD, S>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    AxumListParams(params): AxumListParams<S::Column>,
) -> Result<impl IntoResponse, S::Error>
where
    CRUD: CrudableAxum,
    CRUD::Pkey: From<CRUD::PkeyDe>,
    S: CrudableAxumStateListExt<CRUD>,
    S::Error: Send,
    S::AxumCtx: Send + Sync,
    S::InnerCtx: Send + Sync,
{
    with_ctx(state, ax_ctx, |s, ctx, sh| async move {
        let out = s.handler_list().read_list(params, ctx, sh).await?;
        Ok(Json(into_owned_vec(out)))
    })
    .await
}

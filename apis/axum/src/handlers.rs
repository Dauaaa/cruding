use super::state::{CrudableAxumState, CrudableAxumStateListExt, into_owned_vec};
use super::types::CrudableAxum;
use crate::extractors::{AxumListParams, VecOrSingle};
use axum::response::IntoResponse;
use axum::{Json, extract::State};

fn with_ctx<S: CrudableAxumState, R>(
    state: S,
    ax_ctx: S::AxumCtx,
    f: impl FnOnce(S, super::types::ReqCtx<S::AxumCtx, S::InnerCtx>, S::SourceHandle) -> R,
) -> R {
    let sh = state.new_source_handle();
    let ctx = (ax_ctx, state.inner_ctx());
    f(state, ctx, sh)
}

pub async fn create<S: CrudableAxumState>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    Json(VecOrSingle(items)): Json<VecOrSingle<S::CRUD>>,
) -> Result<impl IntoResponse, S::Error>
where
    <S as CrudableAxumState>::Error: Send,
    <S as CrudableAxumState>::AxumCtx: Send,
{
    with_ctx(state, ax_ctx, |s, mut ctx, mut sh| async move {
        let out = s.handler().create(items, &mut ctx, &mut sh).await?;
        Ok(Json(into_owned_vec(out)))
    })
    .await
}

pub async fn read<S: CrudableAxumState>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    Json(VecOrSingle(keys)): Json<VecOrSingle<<S::CRUD as CrudableAxum>::PkeyDe>>,
) -> Result<impl IntoResponse, S::Error>
where
    <S as CrudableAxumState>::Error: Send,
    <S as CrudableAxumState>::AxumCtx: Send,
{
    let keys = keys.into_iter().map(Into::into).collect();
    with_ctx(state, ax_ctx, |s, mut ctx, mut sh| async move {
        let out = s.handler().read(keys, &mut ctx, &mut sh).await?;
        Ok(Json(into_owned_vec(out)))
    })
    .await
}

pub async fn update<S: CrudableAxumState>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    Json(VecOrSingle(items)): Json<VecOrSingle<S::CRUD>>,
) -> Result<impl IntoResponse, S::Error>
where
    <S as CrudableAxumState>::Error: Send,
    <S as CrudableAxumState>::AxumCtx: Send,
{
    with_ctx(state, ax_ctx, |s, mut ctx, mut sh| async move {
        let out = s.handler().update(items, &mut ctx, &mut sh).await?;
        Ok(Json(into_owned_vec(out)))
    })
    .await
}

pub async fn delete<S: CrudableAxumState>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    Json(VecOrSingle(keys)): Json<VecOrSingle<<S::CRUD as CrudableAxum>::PkeyDe>>,
) -> Result<impl IntoResponse, S::Error>
where
    <S as CrudableAxumState>::Error: Send,
    <S as CrudableAxumState>::AxumCtx: Send,
{
    let keys = keys.into_iter().map(Into::into).collect();
    with_ctx(state, ax_ctx, |s, mut ctx, mut sh| async move {
        s.handler().delete(keys, &mut ctx, &mut sh).await?;
        Ok(axum::http::StatusCode::NO_CONTENT)
    })
    .await
}

pub async fn read_list<S: CrudableAxumStateListExt>(
    State(state): State<S>,
    ax_ctx: S::AxumCtx,
    AxumListParams(params): AxumListParams<S::Column>,
) -> Result<impl IntoResponse, S::Error>
where
    <S as CrudableAxumState>::Error: Send,
    <S as CrudableAxumState>::AxumCtx: Send,
{
    with_ctx(state, ax_ctx, |s, mut ctx, mut sh| async move {
        let out = s
            .handler_list()
            .read_list(params, &mut ctx, &mut sh)
            .await?;
        Ok(Json(into_owned_vec(out)))
    })
    .await
}

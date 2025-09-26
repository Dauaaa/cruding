use super::types::CrudableAxum;
use axum::extract::FromRequestParts;
use axum::response::IntoResponse;

pub trait CrudableAxumState<CRUD>:
    cruding_core::handler::CrudableHandlerGetter<
        CRUD,
        super::types::ReqCtx<Self::AxumCtx, Self::InnerCtx>,
        Self::SourceHandle,
        Self::Error,
    > + Clone
where
    CRUD: CrudableAxum,
    <CRUD as cruding_core::Crudable>::Pkey: From<<CRUD as CrudableAxum>::PkeyDe>,
{
    type AxumCtx: FromRequestParts<Self>;
    type InnerCtx: Send + 'static;
    type SourceHandle: Send;
    type Error: IntoResponse;

    const CRUD_NAME: &'static str;

    fn new_source_handle(&self) -> Self::SourceHandle;
    fn inner_ctx(&self) -> Self::InnerCtx;
}

/// Optional list extension
pub trait CrudableAxumStateListExt<CRUD>:
    CrudableAxumState<CRUD>
    + cruding_core::handler::CrudableHandlerGetterListExt<
        CRUD,
        super::types::ReqCtx<Self::AxumCtx, Self::InnerCtx>,
        Self::SourceHandle,
        Self::Error,
        Self::Column,
    >
where
    CRUD: CrudableAxum,
    <CRUD as cruding_core::Crudable>::Pkey: From<<CRUD as CrudableAxum>::PkeyDe>,
{
    type Column: super::types::ColumnParse;
}

// Small utility to normalize MaybeArc<Vec<T>> → Vec<T>
pub fn into_owned_vec<T: Clone>(v: Vec<cruding_core::handler::MaybeArc<T>>) -> Vec<T> {
    v.into_iter()
        .map(|m| match m {
            cruding_core::handler::MaybeArc::Arced(a) => (*a).clone(),
            cruding_core::handler::MaybeArc::Owned(x) => x,
        })
        .collect()
}

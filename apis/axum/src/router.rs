use crate::types::CrudableAxum;

use super::state::{CrudableAxumState, CrudableAxumStateListExt};
use axum::{
    Router,
    routing::{get, post},
};

pub struct CrudRouter;

impl CrudRouter {
    pub fn base<CRUD, S>() -> Router<S>
    where
        CRUD: CrudableAxum,
        CRUD::Pkey: From<CRUD::PkeyDe>,
        S: CrudableAxumState<CRUD> + Send + Sync + 'static,
        S::AxumCtx: Send,
        S::Error: Send,
    {
        Router::new()
            .route("/create", post(super::handlers::create::<CRUD, S>))
            .route("/read", post(super::handlers::read::<CRUD, S>))
            .route("/update", post(super::handlers::update::<CRUD, S>))
            .route("/delete", post(super::handlers::delete::<CRUD, S>))
    }

    pub fn with_list<CRUD, S>() -> Router<S>
    where
        CRUD: CrudableAxum,
        CRUD::Pkey: From<CRUD::PkeyDe>,
        S: CrudableAxumStateListExt<CRUD> + Send + Sync + 'static,
        S::AxumCtx: Send,
        S::Error: Send,
    {
        Self::base::<CRUD, S>().route("/list", get(super::handlers::read_list::<CRUD, S>))
    }

    pub fn nested<CRUD, S>(plugins: Option<Router<S>>) -> Router<S>
    where
        CRUD: CrudableAxum,
        CRUD::Pkey: From<CRUD::PkeyDe>,
        S: CrudableAxumState<CRUD> + Send + Sync + 'static,
        S::AxumCtx: Send,
        S::Error: Send,
    {
        let r = Self::base::<CRUD, S>();
        let r = if let Some(p) = plugins { r.merge(p) } else { r };
        Router::new().nest(S::CRUD_NAME, r)
    }

    pub fn nested_with_list<CRUD, S>(plugins: Option<Router<S>>) -> Router<S>
    where
        CRUD: CrudableAxum,
        CRUD::Pkey: From<CRUD::PkeyDe>,
        S: CrudableAxumStateListExt<CRUD> + Send + Sync + 'static,
        S::AxumCtx: Send,
        S::Error: Send,
    {
        let r = Self::with_list::<CRUD, S>();
        let r = if let Some(p) = plugins { r.merge(p) } else { r };
        Router::new().nest(S::CRUD_NAME, r)
    }
}

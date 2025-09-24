use super::state::{CrudableAxumState, CrudableAxumStateListExt};
use axum::{
    Router,
    routing::{get, post},
};

pub struct CrudRouter;

impl CrudRouter {
    pub fn base<S>() -> Router<S>
    where
        S: CrudableAxumState + Send + Sync + 'static,
        S::AxumCtx: Send,
        S::Error: Send,
    {
        Router::new()
            .route("/create", post(super::handlers::create::<S>))
            .route("/read", post(super::handlers::read::<S>))
            .route("/update", post(super::handlers::update::<S>))
            .route("/delete", post(super::handlers::delete::<S>))
    }

    pub fn with_list<S>() -> Router<S>
    where
        S: CrudableAxumStateListExt + Send + Sync + 'static,
        S::AxumCtx: Send,
        S::Error: Send,
    {
        Self::base::<S>().route("/list", get(super::handlers::read_list::<S>))
    }

    pub fn nested<S>(plugins: Option<Router<S>>) -> Router<S>
    where
        S: CrudableAxumState + Send + Sync + 'static,
        S::AxumCtx: Send,
        S::Error: Send,
    {
        let r = Self::base::<S>();
        let r = if let Some(p) = plugins { r.merge(p) } else { r };
        Router::new().nest(S::CRUD_NAME, r)
    }

    pub fn nested_with_list<S>(plugins: Option<Router<S>>) -> Router<S>
    where
        S: CrudableAxumStateListExt + Send + Sync + 'static,
        S::AxumCtx: Send,
        S::Error: Send,
    {
        let r = Self::with_list::<S>();
        let r = if let Some(p) = plugins { r.merge(p) } else { r };
        Router::new().nest(S::CRUD_NAME, r)
    }
}

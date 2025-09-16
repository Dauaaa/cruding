use async_trait::async_trait;
use cruding_core::{handler::{CrudableHandler, CrudableHandlerGetter, MaybeArc}, Crudable};
use serde::{Deserialize, Serialize};

pub trait CrudableAxum: Serialize + for<'de> Deserialize<'de> {}
pub trait CrudableAxumState<CRUD, Ctx, SourceHandle, Error>: CrudableHandlerGetter<CRUD, Ctx, SourceHandle, Error> + Clone {
}

pub fn x<CRUD: Crudable>(a: impl CrudableAxumState<CRUD, (), () , ()>) {
}

//pub struct CrudingAxumServer<
//> {
//
//}

//! Defines the core structure of the cruding crate. While the core is void of any details of
//! postgres, redis etc. The core needs to be "aligned" with those systems. This is done by
//! considering that every Crudable has a PRIMARY KEY.

pub mod handler;
pub mod hook;
pub mod list;

use async_trait::async_trait;
use std::{hash::Hash, sync::Arc};

use crate::hook::CrudableHook;

pub trait Crudable: Clone + Send + Sync + 'static {
    type Pkey: Clone + Eq + Hash + Send + Sync + 'static;
    type MonoField: PartialOrd + Send + Sync + 'static;

    fn pkey(&self) -> Self::Pkey;
    fn mono_field(&self) -> Self::MonoField;
}

pub enum CrudableInvalidateCause {
    Update,
    Delete,
}

/// This interface is very dependent on the Handler's behavior. All comments are about
/// [`handler::CrudableHandlerImpl`] usage of CrudableMap.
#[async_trait]
pub trait CrudableMap<CRUD: Crudable>: Clone + Send + Sync + 'static {
    /// Updates the cache but only does that if item's mono is greater.
    /// This will still return Arc<item> even if it lost to the current inserted entry.
    ///
    /// The behavior for inserting should
    async fn insert(&self, items: Vec<CRUD>) -> Vec<Arc<CRUD>>;
    /// This will be called on update/delete and will always be called with the mono of the
    /// updated/deleted item.
    ///
    /// The behavior for invalidating should remove all entries with mono <=
    async fn invalidate<'a, It>(&self, keys: It, cause: CrudableInvalidateCause)
    where
        It: IntoIterator<Item = (&'a CRUD::Pkey, &'a <CRUD as Crudable>::MonoField)> + Clone + Send,
        <It as IntoIterator>::IntoIter: Send;
    /// The order of elements found will be the same as the corresponding keys provided as input.
    /// And if the key didn't exist in the db, corresponding value in the vec will be None.
    async fn get<'a, It>(&self, keys: It) -> Vec<Option<Arc<CRUD>>>
    where
        It: IntoIterator<Item = &'a CRUD::Pkey> + Send,
        <It as IntoIterator>::IntoIter: Send;
}

#[async_trait]
pub trait CrudableSource<CRUD: Crudable>: Clone + Send + Sync + 'static {
    type Error: Send + Sync + 'static;
    type SourceHandle: Send + Sync + 'static;

    async fn create(
        &self,
        items: Vec<CRUD>,
        handle: Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error>;
    async fn read(
        &self,
        keys: &[CRUD::Pkey],
        handle: Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error>;
    async fn update(
        &self,
        items: UpdateComparingParams<CRUD>,
        handle: Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error>;
    async fn read_for_update(
        &self,
        keys: &[CRUD::Pkey],
        handle: Self::SourceHandle,
    ) -> Result<Vec<Arc<CRUD>>, Self::Error> {
        Ok(self
            .read(keys, handle)
            .await?
            .into_iter()
            .map(Arc::new)
            .collect())
    }
    async fn delete(
        &self,
        keys: &[CRUD::Pkey],
        handle: Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error>;

    /// Hints the handler if it should use the cache given the current context. This is useful
    /// if, for example, a database implementation is under a transaction (so whatever the db
    /// returns could be tainted with uncommited changes).
    async fn should_use_cache(&self, handle: Self::SourceHandle) -> bool;
    /// Hints the handler if it can use a batch system (using the provided source handle to the
    /// handler on startup)
    async fn can_use_batcher(&self, handle: Self::SourceHandle) -> bool;
}

pub struct UpdateComparingParams<CRUD: Crudable> {
    // not Arc<_> because we're always fetching from source
    pub current: Vec<Arc<CRUD>>,
    pub update_payload: Vec<CRUD>,
}

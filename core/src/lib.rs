//! Defines the core structure of the cruding crate. While the core is void of any details of
//! postgres, redis etc. The core needs to be "aligned" with those systems. This is done by
//! considering that every Crudable has a PRIMARY KEY.

pub mod handler;
pub mod hook;

use async_trait::async_trait;
use std::{hash::Hash, sync::Arc};

use crate::hook::CrudableHook;

pub trait Crudable: Clone + Send + Sync + 'static {
    type Pkey: Clone + Eq + Hash + Send + Sync + 'static;
    type MonoField: PartialOrd + Send + Sync + 'static;

    fn pkey(&self) -> Self::Pkey;
    fn mono_field(&self) -> Self::MonoField;
}

#[mockall::automock]
#[async_trait]
pub trait CrudableMap<CRUD: Crudable>: Send + Sync + 'static {
    /// Updates the cache but only does that if item's mono is greater. This will still return
    /// Arc<item> even if it lost to the current inserted entry.
    async fn insert(&self, item: CRUD) -> Arc<CRUD>;
    async fn invalidate(&self, key: &CRUD::Pkey);
    async fn get(&self, key: &CRUD::Pkey) -> Option<Arc<CRUD>>;
    async fn contains_key(&self, key: &CRUD::Pkey) -> bool;
}

#[mockall::automock(type Error = (); type SourceHandle = ();)]
#[async_trait]
pub trait CrudableSource<CRUD: Crudable>: Send + Sync + 'static {
    type Error: Send + Sync + 'static;
    type SourceHandle: Send + Sync + 'static;

    async fn create(&self, items: Vec<CRUD>, handle: &mut Self::SourceHandle)
    -> Result<Vec<CRUD>, Self::Error>;
    async fn read(
        &self,
        keys: &[CRUD::Pkey],
        handle: &mut Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error>;
    async fn update(&self, items: Vec<CRUD>, handle: &mut Self::SourceHandle)
    -> Result<Vec<CRUD>, Self::Error>;
    async fn read_for_update(
        &self,
        keys: &[CRUD::Pkey],
        handle: &mut Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error> {
        self.read(keys, handle).await
    }
    async fn delete(
        &self,
        keys: &[CRUD::Pkey],
        handle: &mut Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error>;

    /// Hints the handler if it should use the cache given the current context. This is useful
    /// if, for example, a database implementation is under a transaction (so whatever the db
    /// returns could be tainted with uncommited changes).
    fn use_cache(&self, handle: &Self::SourceHandle) -> bool;
}

pub struct UpdateComparingParams<CRUD: Crudable> {
    // not Arc<_> because we're always fetching from source
    pub current: Vec<CRUD>,
    pub update_payload: Vec<CRUD>,
}

#[async_trait]
impl<CRUD: Crudable> CrudableMap<CRUD> for moka::future::Cache<CRUD::Pkey, Arc<arc_swap::ArcSwap<CRUD>>> {
    async fn insert(&self, item: CRUD) -> Arc<CRUD> {
        let new_item = Arc::new(item);
        let key = new_item.pkey();

        // Guarantee that new_item is latest or not included in cache
        let entry = self.entry(key).or_insert_with(async { Arc::new(arc_swap::ArcSwap::new(new_item.clone())) }).await;
        entry.value().rcu(|cur| {
            if cur.mono_field() < new_item.mono_field() {
                new_item.clone()
            } else {
                cur.clone()
            }
        });

        new_item
    }

    async fn invalidate(&self, key: &CRUD::Pkey) {
        self.invalidate(key).await;
    }

    async fn get(&self, key: &CRUD::Pkey) -> Option<Arc<CRUD>> {
        self.get(key).await.map(|x| x.load_full())
    }

    async fn contains_key(&self, key: &CRUD::Pkey) -> bool {
        self.contains_key(key)
    }
}

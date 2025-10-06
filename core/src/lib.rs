//! Defines the core structure of the cruding crate. While the core is void of any details of
//! postgres, redis etc. The core needs to be "aligned" with those systems. This is done by
//! considering that every Crudable has a PRIMARY KEY.
//!
//! # Cache Backends
//!
//! This crate provides multiple caching backends:
//!
//! ## Moka Cache (In-Memory)
//! - Fast, in-memory caching with LRU eviction
//! - Configurable TTL and TTI
//! - Single-instance only (not shared between processes)
//!
//! ## Redis Cache (Distributed)
//! - Persistent, distributed caching
//! - Shared between multiple instances
//! - Network overhead but better for scaling
//!
//! ## Hybrid Cache (Moka + Redis)
//! - Best of both worlds
//! - Moka as L1 cache (fast), Redis as L2 cache (shared)
//! - Automatic promotion/demotion between layers
//!
//! # Example Usage
//!
//! ```rust,no_run
//! use cruding_core::{HybridCrudableMap, CacheConfig, MokaConfig, RedisConfig};
//! 
//! # #[derive(Clone, serde::Serialize, serde::Deserialize)]
//! # struct MyEntity { id: u64, version: u64 }
//! # impl cruding_core::Crudable for MyEntity {
//! #     type Pkey = u64;
//! #     type MonoField = u64;
//! #     fn pkey(&self) -> u64 { self.id }
//! #     fn mono_field(&self) -> u64 { self.version }
//! # }
//!
//! # async fn example() {
//! // Moka-only cache
//! let moka_cache = HybridCrudableMap::<MyEntity>::moka_only(MokaConfig::default());
//!
//! // Redis-only cache  
//! let redis_cache = HybridCrudableMap::<MyEntity>::redis_only(RedisConfig::default());
//! if let Ok(cache) = redis_cache {
//!     // Use the Redis cache
//! }
//!
//! // Hybrid cache (both Moka and Redis)
//! let hybrid_cache = HybridCrudableMap::<MyEntity>::both(
//!     MokaConfig::default(),
//!     RedisConfig::default(),
//! );
//! if let Ok(cache) = hybrid_cache {
//!     // Use the hybrid cache
//! }
//! # }
//! ```

pub mod handler;
pub mod hook;
pub mod list;
pub mod redis_cache;
pub mod hybrid_cache;

#[cfg(test)]
mod hybrid_cache_tests;

use async_trait::async_trait;
use std::{hash::Hash, sync::Arc};

use crate::hook::CrudableHook;

pub use moka;

pub trait Crudable: Clone + Send + Sync + 'static {
    type Pkey: Clone + Eq + Hash + Send + Sync + 'static;
    type MonoField: PartialOrd + Send + Sync + 'static;

    fn pkey(&self) -> Self::Pkey;
    fn mono_field(&self) -> Self::MonoField;
}

#[async_trait]
pub trait CrudableMap<CRUD: Crudable>: Clone + Send + Sync + 'static {
    /// Updates the cache but only does that if item's mono is greater.
    /// This will still return Arc<item> even if it lost to the current inserted entry.
    async fn insert(&self, items: Vec<CRUD>) -> Vec<Arc<CRUD>>;
    async fn invalidate(&self, keys: &[CRUD::Pkey]);
    /// The order of elements found will be the same as the corresponding keys provided as input.
    /// And if the key didn't exist in the db, corresponding value in the vec will be None.
    async fn get(&self, keys: &[CRUD::Pkey]) -> Vec<Option<Arc<CRUD>>>;
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
        items: Vec<CRUD>,
        handle: Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error>;
    async fn read_for_update(
        &self,
        keys: &[CRUD::Pkey],
        handle: Self::SourceHandle,
    ) -> Result<Vec<CRUD>, Self::Error> {
        self.read(keys, handle).await
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
}

pub struct UpdateComparingParams<CRUD: Crudable> {
    // not Arc<_> because we're always fetching from source
    pub current: Vec<CRUD>,
    pub update_payload: Vec<CRUD>,
}

pub type MokaFutureCrudableMap<CRUD> =
    moka::future::Cache<<CRUD as Crudable>::Pkey, Arc<arc_swap::ArcSwap<CRUD>>>;

#[async_trait]
impl<CRUD: Crudable> CrudableMap<CRUD>
    for moka::future::Cache<CRUD::Pkey, Arc<arc_swap::ArcSwap<CRUD>>>
{
    async fn insert(&self, items: Vec<CRUD>) -> Vec<Arc<CRUD>> {
        let mut results = Vec::with_capacity(items.len());

        for item in items {
            let new_item = Arc::new(item);
            let key = new_item.pkey();

            // Guarantee that new_item is latest or not included in cache
            let entry =
                moka::future::Cache::<CRUD::Pkey, Arc<arc_swap::ArcSwap<CRUD>>>::entry(self, key)
                    .or_insert_with(async { Arc::new(arc_swap::ArcSwap::new(new_item.clone())) })
                    .await;

            entry.value().rcu(|cur| {
                if cur.mono_field() < new_item.mono_field() {
                    new_item.clone()
                } else {
                    cur.clone()
                }
            });

            results.push(new_item);
        }

        results
    }

    async fn invalidate(&self, keys: &[CRUD::Pkey]) {
        for key in keys {
            moka::future::Cache::<CRUD::Pkey, Arc<arc_swap::ArcSwap<CRUD>>>::invalidate(self, key)
                .await;
        }
    }

    async fn get(&self, keys: &[CRUD::Pkey]) -> Vec<Option<Arc<CRUD>>> {
        let mut results = Vec::with_capacity(keys.len());

        for key in keys {
            let item =
                moka::future::Cache::<CRUD::Pkey, Arc<arc_swap::ArcSwap<CRUD>>>::get(self, key)
                    .await
                    .map(|x| x.load_full());
            results.push(item);
        }

        results
    }
}

// Re-export redis and hybrid cache types for easy access
pub use redis_cache::{RedisCrudableMap, RedisConfig};
pub use hybrid_cache::{HybridCrudableMap, CacheConfig, MokaConfig};

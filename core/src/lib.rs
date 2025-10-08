//! Defines the core structure of the cruding crate. While the core is void of any details of
//! postgres, redis etc. The core needs to be "aligned" with those systems. This is done by
//! considering that every Crudable has a PRIMARY KEY.

pub mod handler;
pub mod hook;
pub mod list;
pub mod redis_cache;


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

#[derive(Clone)]
pub struct CrudableMapWithMoka<CRUD: Crudable> {
    redis: redis_cache::RedisCrudableMap<CRUD>,
    moka: Option<MokaFutureCrudableMap<CRUD>>,
}

impl<CRUD> CrudableMapWithMoka<CRUD>
where
    CRUD: Crudable + serde::Serialize + for<'de> serde::Deserialize<'de>,
    CRUD::Pkey: redis::ToRedisArgs + redis::FromRedisValue + std::fmt::Debug,
    CRUD::MonoField: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    pub fn new(
        redis_config: redis_cache::RedisConfig,
        moka: Option<MokaFutureCrudableMap<CRUD>>,
    ) -> Result<Self, redis::RedisError> {
        let redis = redis_cache::RedisCrudableMap::new(redis_config)?;

        Ok(Self {
            redis,
            moka,
        })
    }

    pub fn redis_only(redis_config: redis_cache::RedisConfig) -> Result<Self, redis::RedisError> {
        Ok(Self {
            redis: redis_cache::RedisCrudableMap::new(redis_config)?,
            moka: None,
        })
    }

    pub fn with_moka(
        redis_config: redis_cache::RedisConfig,
        moka: MokaFutureCrudableMap<CRUD>,
    ) -> Result<Self, redis::RedisError> {
        Ok(Self {
            redis: redis_cache::RedisCrudableMap::new(redis_config)?,
            moka: Some(moka),
        })
    }
}

#[async_trait]
impl<CRUD> CrudableMap<CRUD> for CrudableMapWithMoka<CRUD>
where
    CRUD: Crudable + serde::Serialize + for<'de> serde::Deserialize<'de>,
    CRUD::Pkey: redis::ToRedisArgs + redis::FromRedisValue + std::fmt::Debug,
    CRUD::MonoField: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    async fn insert(&self, items: Vec<CRUD>) -> Vec<Arc<CRUD>> {
        if let Some(moka) = &self.moka {
            CrudableMap::insert(moka, items.clone()).await;
        }
        self.redis.insert(items).await
    }

    async fn invalidate(&self, keys: &[CRUD::Pkey]) {
        if let Some(moka) = &self.moka {
            CrudableMap::invalidate(moka, keys).await;
        }
        CrudableMap::invalidate(&self.redis, keys).await;
    }

    async fn get(&self, keys: &[CRUD::Pkey]) -> Vec<Option<Arc<CRUD>>> {
        
        if let Some(moka) = &self.moka {
            let mut results = CrudableMap::get(moka, keys).await;

            let mut redis_keys_to_fetch = Vec::new();
            let mut redis_indices = Vec::new();

            // For the keys not found in moka, getting from redis.
            for (i, result) in results.iter().enumerate() {
                if result.is_none() {
                    redis_keys_to_fetch.push(keys[i].clone());
                    redis_indices.push(i);
                }
            }

            if !redis_keys_to_fetch.is_empty() {
                let redis_results = CrudableMap::get(&self.redis, &redis_keys_to_fetch).await;

                let items_to_backfill: Vec<CRUD> = redis_results
                    .iter()
                    .filter_map(|opt| opt.as_ref().map(|arc| (**arc).clone()))
                    .collect();

                if !items_to_backfill.is_empty() {
                    let moka_clone = moka.clone();
                    tokio::spawn(async move {
                        let _ = CrudableMap::insert(&moka_clone, items_to_backfill).await;
                    });
                }

                for (redis_idx, &final_idx) in redis_indices.iter().enumerate() {
                    if let Some(redis_result) = redis_results.get(redis_idx) {
                        results[final_idx] = redis_result.clone();
                    }
                }
            }

            return results;
        }
        
        // No Moka, just use Redis
        CrudableMap::get(&self.redis, keys).await
    }
}

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

// Re-export types for convenience
pub use redis_cache::{RedisCrudableMap, RedisConfig};
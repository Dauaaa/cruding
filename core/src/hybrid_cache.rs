use async_trait::async_trait;
use std::sync::Arc;

use crate::{redis_cache::RedisCrudableMap, Crudable, CrudableMap, MokaFutureCrudableMap};

#[derive(Clone)]
pub struct HybridCrudableMap<CRUD: Crudable> {
    moka: Option<MokaFutureCrudableMap<CRUD>>,
    redis: Option<RedisCrudableMap<CRUD>>,
}

#[derive(Debug, Clone)]
pub struct CacheConfig {
    pub enable_moka: bool,
    pub enable_redis: bool,
    pub moka_config: Option<MokaConfig>,
    pub redis_config: Option<crate::redis_cache::RedisConfig>,
}

#[derive(Debug, Clone)]
pub struct MokaConfig {
    pub max_capacity: u64,
    pub time_to_live: Option<std::time::Duration>,
    pub time_to_idle: Option<std::time::Duration>,
}

impl Default for MokaConfig {
    fn default() -> Self {
        Self {
            max_capacity: 10000,
            time_to_live: None,
            time_to_idle: None,
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enable_moka: true,
            enable_redis: false,
            moka_config: Some(MokaConfig::default()),
            redis_config: None,
        }
    }
}

impl<CRUD> HybridCrudableMap<CRUD>
where
    CRUD: Crudable + serde::Serialize + for<'de> serde::Deserialize<'de>,
    CRUD::Pkey: redis::ToRedisArgs + redis::FromRedisValue + std::fmt::Debug,
    CRUD::MonoField: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    pub fn new(config: CacheConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let moka = if config.enable_moka {
            let moka_config = config.moka_config.unwrap_or_default();
            let mut builder = moka::future::Cache::builder().max_capacity(moka_config.max_capacity);

            if let Some(ttl) = moka_config.time_to_live {
                builder = builder.time_to_live(ttl);
            }

            if let Some(tti) = moka_config.time_to_idle {
                builder = builder.time_to_idle(tti);
            }

            Some(builder.build())
        } else {
            None
        };

        let redis = if config.enable_redis {
            let redis_config = config.redis_config.unwrap_or_default();
            Some(RedisCrudableMap::new(redis_config)?)
        } else {
            None
        };

        if moka.is_none() && redis.is_none() {
            return Err("At least one cache backend must be enabled".into());
        }

        Ok(Self { moka, redis })
    }

    pub fn moka_only(config: MokaConfig) -> Self {
        let mut builder = moka::future::Cache::builder().max_capacity(config.max_capacity);

        if let Some(ttl) = config.time_to_live {
            builder = builder.time_to_live(ttl);
        }

        if let Some(tti) = config.time_to_idle {
            builder = builder.time_to_idle(tti);
        }

        Self {
            moka: Some(builder.build()),
            redis: None,
        }
    }

    pub fn redis_only(
        config: crate::redis_cache::RedisConfig,
    ) -> Result<Self, redis::RedisError> {
        Ok(Self {
            moka: None,
            redis: Some(RedisCrudableMap::new(config)?),
        })
    }

    pub fn both(
        moka_config: MokaConfig,
        redis_config: crate::redis_cache::RedisConfig,
    ) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let mut builder = moka::future::Cache::builder().max_capacity(moka_config.max_capacity);

        if let Some(ttl) = moka_config.time_to_live {
            builder = builder.time_to_live(ttl);
        }

        if let Some(tti) = moka_config.time_to_idle {
            builder = builder.time_to_idle(tti);
        }

        Ok(Self {
            moka: Some(builder.build()),
            redis: Some(RedisCrudableMap::new(redis_config)?),
        })
    }
}

#[async_trait]
impl<CRUD> CrudableMap<CRUD> for HybridCrudableMap<CRUD>
where
    CRUD: Crudable + serde::Serialize + for<'de> serde::Deserialize<'de>,
    CRUD::Pkey: redis::ToRedisArgs + redis::FromRedisValue + std::fmt::Debug,
    CRUD::MonoField: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    async fn insert(&self, items: Vec<CRUD>) -> Vec<Arc<CRUD>> {
        match (&self.moka, &self.redis) {
            (Some(moka), Some(redis)) => {
                let moka_future = CrudableMap::insert(moka, items.clone());
                let redis_future = CrudableMap::insert(redis, items);

                let (moka_result, redis_result) = tokio::join!(moka_future, redis_future);

                if moka_result.len() != redis_result.len() {
                    tracing::warn!("Moka and Redis insert results have different lengths");
                }

                moka_result
            }
            (Some(moka), None) => CrudableMap::insert(moka, items).await,

            (None, Some(redis)) => CrudableMap::insert(redis, items).await,

            (None, None) => {
                tracing::error!("No cache backend enabled");
                items.into_iter().map(Arc::new).collect()
            }
        }
    }

    async fn invalidate(&self, keys: &[CRUD::Pkey]) {
        match (&self.moka, &self.redis) {
            (Some(moka), Some(redis)) => {
                let moka_future = CrudableMap::invalidate(moka, keys);
                let redis_future = CrudableMap::invalidate(redis, keys);

                tokio::join!(moka_future, redis_future);
            }
            (Some(moka), None) => CrudableMap::invalidate(moka, keys).await,
            (None, Some(redis)) => CrudableMap::invalidate(redis, keys).await,
            (None, None) => {
                tracing::error!("No cache backend enabled for invalidation");
            }
        }
    }

    async fn get(&self, keys: &[CRUD::Pkey]) -> Vec<Option<Arc<CRUD>>> {
        match (&self.moka, &self.redis) {
            (Some(moka), Some(redis)) => {
                let moka_results = CrudableMap::get(moka, keys).await;
                let mut final_results = Vec::with_capacity(keys.len());
                let mut redis_keys_to_fetch = Vec::new();
                let mut redis_indices = Vec::new();

                for (i, result) in moka_results.iter().enumerate() {
                    if result.is_some() {
                        final_results.push(result.clone());
                    } else {
                        final_results.push(None);
                        redis_keys_to_fetch.push(keys[i].clone());
                        redis_indices.push(i);
                    }
                }

                if !redis_keys_to_fetch.is_empty() {
                    let redis_results = CrudableMap::get(redis, &redis_keys_to_fetch).await;

                    for (redis_idx, &final_idx) in redis_indices.iter().enumerate() {
                        if let Some(redis_result) = redis_results.get(redis_idx) {
                            final_results[final_idx] = redis_result.clone();

                            if let Some(item) = redis_result {
                                let moka_clone = moka.clone();
                                let item_clone = (**item).clone();
                                tokio::spawn(async move {
                                    let _ = CrudableMap::insert(&moka_clone, vec![item_clone]).await;
                                });
                            }
                        }
                    }
                }

                final_results
            }
            (Some(moka), None) => CrudableMap::get(moka, keys).await,
            (None, Some(redis)) => CrudableMap::get(redis, keys).await,
            (None, None) => {
                tracing::error!("No cache backend enabled for get");
                vec![None; keys.len()]
            }
        }
    }
}
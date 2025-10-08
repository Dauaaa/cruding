use async_trait::async_trait;
use redis::{AsyncCommands, Client, FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use crate::{Crudable, CrudableMap};

#[derive(Clone)]
pub struct RedisCrudableMap<CRUD: Crudable> {
    client: Client,
    key_prefix: String,
    /// When true, insert and invalidate are done asynchronously
    dispatch_mutations: bool,
    _phantom: PhantomData<CRUD>,
}

#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub url: String,
    pub key_prefix: Option<String>,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: Some("cruding:".to_string()),
        }
    }
}

impl<CRUD> RedisCrudableMap<CRUD>
where
    CRUD: Crudable + Serialize + for<'de> Deserialize<'de>,
    CRUD::Pkey: ToRedisArgs + FromRedisValue + Debug,
    CRUD::MonoField: Serialize + for<'de> Deserialize<'de>,
{
    #[tracing::instrument(skip_all)]
    pub fn new(config: RedisConfig) -> Result<Self, redis::RedisError> {
        let client = Client::open(config.url)?;
        let key_prefix = config.key_prefix.unwrap_or_else(|| "cruding:".to_string());
        let dispatch_mutations = true;

        Ok(Self {
            client,
            key_prefix,
            dispatch_mutations,
            _phantom: PhantomData,
        })
    }

    fn make_key(&self, pkey: &CRUD::Pkey) -> String {
        format!("{}data:{:?}", self.key_prefix, pkey)
    }

    fn make_mono_key(&self, pkey: &CRUD::Pkey) -> String {
        format!("{}mono:{:?}", self.key_prefix, pkey)
    }

    async fn get_connection(&self) -> Result<redis::aio::Connection, redis::RedisError> {
        self.client.get_async_connection().await
    }

    async fn insert_sync(&self, items: Vec<CRUD>) -> Vec<Arc<CRUD>> {
        let mut conn = match self.get_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("Failed to get Redis connection: {}", e);
                return items.into_iter().map(Arc::new).collect();
            }
        };

        let mut results = Vec::with_capacity(items.len());
        let mut pipe = redis::pipe();
        pipe.atomic();

        let mono_keys: Vec<String> = items.iter()
            .map(|item| self.make_mono_key(&item.pkey()))
            .collect();

        let existing_monos: Vec<Option<String>> = match conn.get(&mono_keys).await {
            Ok(values) => values,
            Err(e) => {
                tracing::error!("Failed to get existing mono fields: {}", e);
                vec![None; items.len()]
            }
        };

        for (item, existing_mono) in items.into_iter().zip(existing_monos.iter()) {
            let new_item = Arc::new(item);
            let should_update = match existing_mono {
                Some(mono_json) => {
                    match serde_json::from_str::<CRUD::MonoField>(mono_json) {
                        Ok(existing) => new_item.mono_field() > existing,
                        Err(_) => true,
                    }
                }
                None => true,
            };

            if should_update {
                if let (Ok(item_json), Ok(mono_json)) = (
                    serde_json::to_string(&*new_item),
                    serde_json::to_string(&new_item.mono_field()),
                ) {
                    let data_key = self.make_key(&new_item.pkey());
                    let mono_key = self.make_mono_key(&new_item.pkey());
                    pipe.set(&data_key, &item_json).set(&mono_key, &mono_json);
                }
            }

            results.push(new_item);
        }

        if let Err(e) = pipe.query_async::<_, ()>(&mut conn).await {
            tracing::error!("Failed to store items in Redis: {}", e);
        }

        results
    }

    async fn invalidate_sync(&self, keys: &[CRUD::Pkey]) {
        let mut conn = match self.get_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("Failed to get Redis connection for invalidation: {}", e);
                return;
            }
        };

        let mut pipe = redis::pipe();
        pipe.atomic();

        for key in keys {
            let data_key = self.make_key(key);
            let mono_key = self.make_mono_key(key);
            pipe.del(&data_key).del(&mono_key);
        }

        if let Err(e) = pipe.query_async::<_, ()>(&mut conn).await {
            tracing::error!("Failed to invalidate keys in Redis: {}", e);
        }
    }
}

#[async_trait]
impl<CRUD> CrudableMap<CRUD> for RedisCrudableMap<CRUD>
where
    CRUD: Crudable + Serialize + for<'de> Deserialize<'de>,
    CRUD::Pkey: ToRedisArgs + FromRedisValue + Debug,
    CRUD::MonoField: Serialize + for<'de> Deserialize<'de>,
{
    #[tracing::instrument(skip_all)]
    async fn insert(&self, items: Vec<CRUD>) -> Vec<Arc<CRUD>> {
        if self.dispatch_mutations {
            let self_clone = self.clone();
            let items_clone = items.clone();
            tokio::spawn(async move {
                let _ = self_clone.insert_sync(items_clone).await;
            });
            return items.into_iter().map(Arc::new).collect();
        }

        self.insert_sync(items).await
    }

    #[tracing::instrument(skip_all)]
    async fn invalidate(&self, keys: &[CRUD::Pkey]) {
        if self.dispatch_mutations {
            let self_clone = self.clone();
            let keys_clone = keys.to_vec();
            tokio::spawn(async move {
                let _ = self_clone.invalidate_sync(&keys_clone).await;
            });
            return;
        }

        self.invalidate_sync(keys).await
    }

    #[tracing::instrument(skip_all)]
    async fn get(&self, keys: &[CRUD::Pkey]) -> Vec<Option<Arc<CRUD>>> {
        let mut results = Vec::with_capacity(keys.len());
        let mut conn = match self.get_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("Failed to get Redis connection for get: {}", e);
                return vec![None; keys.len()];
            }
        };

        for key in keys {
            let data_key = self.make_key(key);

            match conn.get::<_, Option<String>>(&data_key).await {
                Ok(Some(item_json)) => {
                    match serde_json::from_str::<CRUD>(&item_json) {
                        Ok(item) => results.push(Some(Arc::new(item))),
                        Err(e) => {
                            tracing::error!("Failed to deserialize item from Redis: {}", e);
                            results.push(None);
                        }
                    }
                }
                Ok(None) => results.push(None),
                Err(e) => {
                    tracing::error!("Failed to get item from Redis: {}", e);
                    results.push(None);
                }
            }
        }

        results
    }
}
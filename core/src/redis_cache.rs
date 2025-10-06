use async_trait::async_trait;
use redis::{AsyncCommands, Client, FromRedisValue, ToRedisArgs};
use serde::{Deserialize, Serialize};
use std::{fmt::Debug, marker::PhantomData, sync::Arc};

use crate::{Crudable, CrudableMap};

#[derive(Clone)]
pub struct RedisCrudableMap<CRUD: Crudable> {
    client: Client,
    key_prefix: String,
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

        Ok(Self {
            client,
            key_prefix,
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
        let mut results = Vec::with_capacity(items.len());
        let mut conn = match self.get_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("Failed to get Redis connection: {}", e);
                return items.into_iter().map(Arc::new).collect();
            }
        };

        for item in items {
            let new_item = Arc::new(item);
            let key = new_item.pkey();
            let data_key = self.make_key(&key);
            let mono_key = self.make_mono_key(&key);

            let should_update = match conn.get::<_, Option<String>>(&mono_key).await {
                Ok(Some(existing_mono_json)) => {
                    match serde_json::from_str::<CRUD::MonoField>(&existing_mono_json) {
                        Ok(existing_mono) => new_item.mono_field() > existing_mono,
                        Err(_) => true,
                    }
                }
                Ok(None) => true,
                Err(e) => {
                    tracing::error!("Failed to check monotonic field: {}", e);
                    true
                }
            };

            if should_update {
                match (
                    serde_json::to_string(&*new_item),
                    serde_json::to_string(&new_item.mono_field()),
                ) {
                    (Ok(item_json), Ok(mono_json)) => {
                        let result: Result<(), redis::RedisError> = redis::pipe()
                            .atomic()
                            .set(&data_key, &item_json)
                            .set(&mono_key, &mono_json)
                            .query_async(&mut conn)
                            .await;

                        if let Err(e) = result {
                            tracing::error!("Failed to store item in Redis: {}", e);
                        }
                    }
                    (Err(e), _) | (_, Err(e)) => {
                        tracing::error!("Failed to serialize item: {}", e);
                    }
                }
            }

            results.push(new_item);
        }

        results
    }

    #[tracing::instrument(skip_all)]
    async fn invalidate(&self, keys: &[CRUD::Pkey]) {
        let mut conn = match self.get_connection().await {
            Ok(conn) => conn,
            Err(e) => {
                tracing::error!("Failed to get Redis connection for invalidation: {}", e);
                return;
            }
        };

        for key in keys {
            let data_key = self.make_key(key);
            let mono_key = self.make_mono_key(key);

            let result: Result<(), redis::RedisError> = redis::pipe()
                .atomic()
                .del(&data_key)
                .del(&mono_key)
                .query_async(&mut conn)
                .await;

            if let Err(e) = result {
                tracing::error!("Failed to invalidate key in Redis: {}", e);
            }
        }
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
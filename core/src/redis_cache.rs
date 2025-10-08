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
    /// SHA hash of the compare-and-set Lua script
    script_sha: Arc<String>,
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
    pub async fn new(config: RedisConfig) -> Result<Self, redis::RedisError> {
        let client = Client::open(config.url)?;
        let key_prefix = config.key_prefix.unwrap_or_else(|| "cruding:".to_string());
        let dispatch_mutations = true;

        // Load Lua script for atomic compare-and-set
        let mut conn = client.get_async_connection().await?;
        
        let script = r#"
            local existing_mono = redis.call('GET', KEYS[2])
            
            if not existing_mono then
                redis.call('SET', KEYS[1], ARGV[1])
                redis.call('SET', KEYS[2], ARGV[2])
                return 1
            end
            
            local existing_val = cjson.decode(existing_mono)
            local new_val = cjson.decode(ARGV[2])
            
            if new_val > existing_val then
                redis.call('SET', KEYS[1], ARGV[1])
                redis.call('SET', KEYS[2], ARGV[2])
                return 1
            end
            
            return 0
        "#;
        
        let sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(script)
            .query_async(&mut conn)
            .await?;

        Ok(Self {
            client,
            key_prefix,
            dispatch_mutations,
            script_sha: Arc::new(sha),
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

        let mut pipe = redis::pipe();

        // Build all EVALSHA commands in a pipeline
        for item in &items {
            let data_key = self.make_key(&item.pkey());
            let mono_key = self.make_mono_key(&item.pkey());

            if let (Ok(item_json), Ok(mono_json)) = (
                serde_json::to_string(item),
                serde_json::to_string(&item.mono_field()),
            ) {
                pipe.cmd("EVALSHA")
                    .arg(&*self.script_sha)
                    .arg(2)
                    .arg(&data_key)
                    .arg(&mono_key)
                    .arg(&item_json)
                    .arg(&mono_json);
            }
        }

        // Execute all EVALSHA commands at once
        if let Err(e) = pipe.query_async::<_, Vec<i32>>(&mut conn).await {
            tracing::error!("Failed to execute batch EVALSHA: {}", e);
        }

        // Return all items as Arc
        items.into_iter().map(Arc::new).collect()
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
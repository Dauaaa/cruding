use async_trait::async_trait;
use cruding_core::{Crudable, CrudableInvalidateCause, CrudableMap};
use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

pub use moka;

pub type MokaCache<Key, CRUD> = moka::future::Cache<Key, Arc<arc_swap::ArcSwap<CRUD>>>;
pub type MokaCacheBuilder<Key, CRUD> =
    moka::future::CacheBuilder<Key, Arc<arc_swap::ArcSwap<CRUD>>, MokaCache<Key, CRUD>>;

/// A cache backed by moka for reading and DashMap for invalidation and updates
pub struct CrudingInMemoryCache<CRUD: Crudable> {
    moka_cache: MokaCache<CRUD::Pkey, CRUD>,
    mono_map: Arc<DashMap<CRUD::Pkey, CRUD::MonoField>>,
}

impl<CRUD: Crudable> Clone for CrudingInMemoryCache<CRUD> {
    fn clone(&self) -> Self {
        Self {
            moka_cache: self.moka_cache.clone(),
            mono_map: self.mono_map.clone(),
        }
    }
}

impl<CRUD: Crudable> CrudingInMemoryCache<CRUD> {
    pub async fn new(cache_builder: MokaCacheBuilder<CRUD::Pkey, CRUD>) -> Self {
        let mono_map = Arc::new(DashMap::new());
        let moka_cache_ref: Arc<RwLock<Option<MokaCache<CRUD::Pkey, CRUD>>>> =
            Arc::new(RwLock::new(None));

        let moka_cache = cache_builder.async_eviction_listener({
            let mono_map = mono_map.clone();
            let moka_cache_ref = moka_cache_ref.clone();
            move |key, crud, cause| {
                let mono_map = mono_map.clone();
                let moka_cache_ref = moka_cache_ref.clone();
                Box::pin(async move {

            let key = (&*key).clone();

            match cause {
                moka::notification::RemovalCause::Replaced | moka::notification::RemovalCause::Expired | moka::notification::RemovalCause::Size => {
                    match mono_map.entry(key.clone()) {
                        // after being removed by moka's general rules, we need to also remove the
                        // entry from the dashmap. While doing this we could catch a bug so the
                        // cur_mono < replaced_mono branch tries to rever the cache to a consistent
                        // state
                        dashmap::Entry::Occupied(mut cur_mono) => {
                            let replaced_mono = crud.load().mono_field();
                            if *cur_mono.get() < replaced_mono {
                                tracing::error!("Race condition reached, an entry with more recent mono_field was evicted");
                                if cfg!(test) {
                                    panic!("Race condition reached, an entry with more recent mono_field was evicted");
                                }
                                tracing::error!("Replacing entry with smaller mono...");
                                moka_cache_ref.read().await.as_ref().unwrap().insert(key, crud).await;
                                cur_mono.insert(replaced_mono);
                            } else {
                                cur_mono.remove();
                            }
                        },
                        // can't introspect current status, missing a cache entry is not critical
                        // neither
                        dashmap::Entry::Vacant(_) => {},
                    }
                },
                // we assume explicit removals are always correct
                moka::notification::RemovalCause::Explicit => {},
            }
        })}
        }
            ).build();

        *moka_cache_ref.write().await = Some(moka_cache.clone());

        Self {
            moka_cache,
            mono_map,
        }
    }
}

#[async_trait]
impl<CRUD: Crudable> CrudableMap<CRUD> for CrudingInMemoryCache<CRUD> {
    async fn insert(&self, items: Vec<CRUD>) -> Vec<Arc<CRUD>> {
        let mut results = Vec::with_capacity(items.len());

        for item in items {
            let key = item.pkey();
            let insert_mono = item.mono_field();
            let new_item = Arc::new(item);

            match self.mono_map.entry(key.clone()) {
                dashmap::Entry::Occupied(mut cur_mono) => {
                    if *cur_mono.get() < insert_mono {
                        cur_mono.insert(insert_mono);
                        self.moka_cache
                            .insert(key, Arc::new(arc_swap::ArcSwap::new(new_item.clone())))
                            .await;
                    }
                }
                dashmap::Entry::Vacant(vacant) => {
                    vacant.insert(insert_mono);
                    self.moka_cache
                        .insert(key, Arc::new(arc_swap::ArcSwap::new(new_item.clone())))
                        .await;
                }
            }

            results.push(new_item);
        }

        results
    }

    async fn invalidate<'a, It>(&self, keys: It, cause: CrudableInvalidateCause)
    where
        It: IntoIterator<Item = (&'a CRUD::Pkey, &'a <CRUD as Crudable>::MonoField)> + Clone + Send,
        <It as IntoIterator>::IntoIter: Send,
    {
        for (key, invalidate_mono) in keys {
            match self.mono_map.entry(key.clone()) {
                dashmap::Entry::Occupied(cur_mono) => match cause {
                    CrudableInvalidateCause::Update if cur_mono.get() < invalidate_mono => {
                        cur_mono.remove();
                        self.moka_cache.invalidate(key).await;
                    }
                    CrudableInvalidateCause::Delete if cur_mono.get() <= invalidate_mono => {
                        cur_mono.remove();
                        self.moka_cache.invalidate(key).await;
                    }
                    _ => {}
                },
                dashmap::Entry::Vacant(_) => {
                    self.moka_cache.invalidate(key).await;
                }
            }
        }
    }

    async fn get<'a, It>(&self, keys: It) -> Vec<Option<Arc<CRUD>>>
    where
        It: IntoIterator<Item = &'a CRUD::Pkey> + Send,
        <It as IntoIterator>::IntoIter: Send,
    {
        let keys = keys.into_iter();
        let (lb, ub) = keys.size_hint();
        let mut results = Vec::with_capacity(ub.unwrap_or(lb));

        for key in keys {
            let item = moka::future::Cache::<CRUD::Pkey, Arc<arc_swap::ArcSwap<CRUD>>>::get(
                &self.moka_cache,
                key,
            )
            .await
            .map(|x| x.load_full());
            results.push(item);
        }

        results
    }
}

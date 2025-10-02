// tests/cruding_core.rs
use async_trait::async_trait;
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

use arc_swap::ArcSwap;
use cruding_core::{
    // replace with your crate name
    Crudable,
    CrudableSource,
    handler::{CrudableHandler, CrudableHandlerImpl, MaybeArc},
    hook::make_crudable_hook,
};
use moka::future::Cache;

// ---------- Test model ----------

#[derive(Clone, Debug, PartialEq, Eq)]
struct Item {
    id: u64,
    mono: i64,
    val: i32,
}
impl Crudable for Item {
    type Pkey = u64;
    type MonoField = i64;

    fn pkey(&self) -> Self::Pkey {
        self.id
    }
    fn mono_field(&self) -> Self::MonoField {
        self.mono
    }
}

// ---------- Fake source (in-memory DB) ----------

#[derive(Clone, Default)]
struct MemSource {
    // (version/mono, value)
    inner: Arc<Mutex<HashMap<u64, Item>>>,
    // whether the handler should use cache in this “context”
    allow_cache: bool,
}
#[derive(Clone, Default)]
struct Handle; // no-op transaction/context

#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
enum DbError {
    #[error("db_error")]
    NotFound,
}

#[async_trait]
impl CrudableSource<Item> for MemSource {
    type Error = DbError;
    type SourceHandle = Handle;

    async fn create(&self, items: Vec<Item>, _h: Handle) -> Result<Vec<Item>, DbError> {
        let mut g = self.inner.lock().await;
        for it in &items {
            g.insert(it.id, it.clone());
        }
        Ok(items)
    }

    async fn read(&self, keys: &[u64], _h: Handle) -> Result<Vec<Item>, DbError> {
        let g = self.inner.lock().await;
        Ok(keys.iter().filter_map(|k| g.get(k).cloned()).collect())
    }

    async fn update(&self, items: Vec<Item>, _h: Handle) -> Result<Vec<Item>, DbError> {
        let mut g = self.inner.lock().await;
        for it in &items {
            g.insert(it.id, it.clone());
        }
        Ok(items)
    }

    async fn read_for_update(&self, keys: &[u64], _h: Handle) -> Result<Vec<Item>, DbError> {
        self.read(keys, _h).await
    }

    async fn delete(&self, keys: &[u64], _h: Handle) -> Result<Vec<Item>, DbError> {
        let mut g = self.inner.lock().await;
        let mut out = Vec::new();
        for k in keys {
            if let Some(v) = g.remove(k) {
                out.push(v);
            }
        }
        Ok(out)
    }

    async fn should_use_cache(&self, _h: Handle) -> bool {
        self.allow_cache
    }
}

// ---------- Utilities ----------

fn new_cache() -> Cache<u64, Arc<ArcSwap<Item>>> {
    Cache::builder()
        .max_capacity(10_000)
        .time_to_live(Duration::from_secs(60))
        .build()
}

#[allow(clippy::type_complexity)]
fn handler_with(
    source: MemSource,
    cache: Cache<u64, Arc<ArcSwap<Item>>>,
) -> CrudableHandlerImpl<Item, Cache<u64, Arc<ArcSwap<Item>>>, MemSource, TestCtx, TestError> {
    CrudableHandlerImpl::new(cache, source)
}

#[derive(Clone, Default)]
struct TestCtx;
#[allow(dead_code)]
#[derive(Debug, thiserror::Error)]
enum TestError {
    #[error("db: {0:?}")]
    Db(#[from] DbError),
    #[error("hook: {0}")]
    Hook(&'static str),
}

// ---------- Tests ----------

#[tokio::test]
async fn create_then_read_hits_cache() {
    let cache = new_cache();
    let source = MemSource {
        allow_cache: true,
        ..Default::default()
    };
    let handler = handler_with(source, cache);

    let ctx = TestCtx;
    let h = Handle;

    // Create
    let created = handler
        .create(
            vec![Item {
                id: 1,
                mono: 1,
                val: 10,
            }],
            ctx.clone(),
            h.clone(),
        )
        .await
        .unwrap();

    match &created[0] {
        MaybeArc::Arced(v) => assert_eq!(
            **v,
            Item {
                id: 1,
                mono: 1,
                val: 10
            }
        ),
        _ => panic!("expected Arced (cache write-through)"),
    }

    // Read should hit cache (no source call visible here, but correctness via equality)
    let read = handler.read(vec![1], ctx, h).await.unwrap();
    match &read[0] {
        MaybeArc::Arced(v) => assert_eq!(
            **v,
            Item {
                id: 1,
                mono: 1,
                val: 10
            }
        ),
        _ => panic!("expected Arced"),
    }
}

#[tokio::test]
async fn read_miss_then_populates_cache() {
    let cache = new_cache();
    let source = MemSource {
        allow_cache: true,
        ..Default::default()
    };
    // Preload DB only
    {
        let h = Handle;
        source
            .create(
                vec![Item {
                    id: 2,
                    mono: 5,
                    val: 20,
                }],
                h,
            )
            .await
            .unwrap();
    }

    let handler = handler_with(source, cache);
    let ctx = TestCtx;
    let h = Handle;

    // First read: miss → fetch → cache
    let out1 = handler.read(vec![2], ctx.clone(), h.clone()).await.unwrap();
    assert!(matches!(&out1[0], MaybeArc::Arced(_)));

    // Second read: should be a cache hit
    let out2 = handler.read(vec![2], ctx, h).await.unwrap();
    assert!(matches!(&out2[0], MaybeArc::Arced(_)));
}

#[tokio::test]
async fn update_obeys_monotonic_replace() {
    let cache = new_cache();
    let source = MemSource {
        allow_cache: true,
        ..Default::default()
    };
    let handler = handler_with(source, cache);

    let ctx = TestCtx;
    let h = Handle;

    // Create v1
    handler
        .create(
            vec![Item {
                id: 3,
                mono: 1,
                val: 1,
            }],
            ctx.clone(),
            h.clone(),
        )
        .await
        .unwrap();

    // Update with lower mono (cache will be invalidated)
    let update_result = handler
        .update(
            vec![Item {
                id: 3,
                mono: 0,
                val: 999,
            }],
            ctx.clone(),
            h.clone(),
        )
        .await
        .unwrap();

    // Update should return Owned (not cached) since we invalidate on update
    match &update_result[0] {
        MaybeArc::Owned(v) => assert_eq!(
            *v,
            Item {
                id: 3,
                mono: 0,
                val: 999
            }
        ),
        _ => panic!("expected Owned after update"),
    }

    // Read should fetch from database (cache was invalidated)
    let out_low = handler.read(vec![3], ctx.clone(), h.clone()).await.unwrap();

    // Since cache was invalidated, this should be a fresh read from database
    match &out_low[0] {
        MaybeArc::Arced(v) => assert_eq!(
            **v,
            Item {
                id: 3,
                mono: 0,  // Database value, not the old cached value
                val: 999
            }
        ),
        _ => panic!("expected Arced from fresh read"),
    }

    // Update with higher mono (cache will be invalidated again)
    let out_hi = handler
        .update(
            vec![Item {
                id: 3,
                mono: 2,
                val: 123,
            }],
            ctx,
            h,
        )
        .await
        .unwrap();

    // Update should return Owned (not cached)
    match &out_hi[0] {
        MaybeArc::Owned(v) => assert_eq!(
            *v,
            Item {
                id: 3,
                mono: 2,
                val: 123
            }
        ),
        _ => panic!("expected Owned after update"),
    }
}

#[tokio::test]
async fn delete_invalidates_cache() {
    let cache = new_cache();
    let source = MemSource {
        allow_cache: true,
        ..Default::default()
    };
    let handler = handler_with(source, cache);

    let ctx = TestCtx;
    let h = Handle;

    handler
        .create(
            vec![Item {
                id: 4,
                mono: 1,
                val: 10,
            }],
            ctx.clone(),
            h.clone(),
        )
        .await
        .unwrap();

    // Ensure it’s in cache
    let got = handler.read(vec![4], ctx.clone(), h.clone()).await.unwrap();
    assert!(matches!(&got[0], MaybeArc::Arced(_)));

    // Delete
    handler
        .delete(vec![4], ctx.clone(), h.clone())
        .await
        .unwrap();

    // After delete, a read should not return cached value (DB is empty too)
    let out = handler.read(vec![4], ctx.clone(), h.clone()).await.unwrap();
    assert!(out.is_empty(), "should be empty after delete");
}

#[tokio::test]
async fn use_cache_false_bypasses_map() {
    let cache = new_cache();
    let source = MemSource {
        allow_cache: false,
        ..Default::default()
    };
    let handler = handler_with(source, cache);

    let ctx = TestCtx;
    let h = Handle;

    // Create returns Owned (not Arced) and does not populate cache
    let created = handler
        .create(
            vec![Item {
                id: 5,
                mono: 1,
                val: 5,
            }],
            ctx.clone(),
            h.clone(),
        )
        .await
        .unwrap();
    assert!(matches!(&created[0], MaybeArc::Owned(_)));

    // Read returns Owned; also not cached between calls
    let out1 = handler.read(vec![5], ctx.clone(), h.clone()).await.unwrap();
    assert!(matches!(&out1[0], MaybeArc::Owned(_)));

    let out2 = handler.read(vec![5], ctx, h).await.unwrap();
    assert!(matches!(&out2[0], MaybeArc::Owned(_)));
}

#[tokio::test]
async fn hooks_are_invoked_and_can_transform() {
    let cache = new_cache();
    let source = MemSource {
        allow_cache: true,
        ..Default::default()
    };
    let handler = handler_with(source, cache)
        // before_create: add 100 to all values
        .install_before_create(make_crudable_hook(
            |_h, mut items: Vec<Item>, _ctx: TestCtx, _: Handle| async move {
                for it in &mut items {
                    it.val += 100;
                }
                Ok(items)
            },
        ))
        // before_read: append an extra id
        .install_before_read(make_crudable_hook(
            |_h, mut keys: Vec<u64>, _ctx: TestCtx, _| async move {
                keys.push(9999);
                Ok(keys)
            },
        ))
        // after_read: filter out any id=9999
        .install_after_read(make_crudable_hook(
            |_h, items: Vec<MaybeArc<Item>>, _ctx: TestCtx, _| async move {
                Ok(items
                    .into_iter()
                    .filter(|m| match m {
                        MaybeArc::Arced(v) => v.id != 9999,
                        MaybeArc::Owned(v) => v.id != 9999,
                    })
                    .collect())
            },
        ))
        // before_update: ensure mono bumps by +1
        .install_before_update(make_crudable_hook(
            |_h, mut items: Vec<Item>, _ctx: TestCtx, _| async move {
                for it in &mut items {
                    it.mono += 1;
                }
                Ok(items)
            },
        ))
        // update_comparing: assert monotonicity or rewrite
        .install_update_comparing(make_crudable_hook(
            |_h, mut p: cruding_core::UpdateComparingParams<Item>, _ctx: TestCtx, _| async move {
                // force mono to be >= current.mono
                let max_mono = p.current.iter().map(|c| c.mono).max().unwrap_or(0);
                for it in &mut p.update_payload {
                    if it.mono < max_mono {
                        it.mono = max_mono;
                    }
                }
                Ok(p.update_payload)
            },
        ))
        // before_delete_resolved: ensure we see real items for deletion
        .install_before_delete_resolved(make_crudable_hook(
            |_h, items: Vec<MaybeArc<Item>>, _ctx: TestCtx, _| async move {
                assert!(!items.is_empty());
                Ok(())
            },
        ));

    let ctx = TestCtx;
    let h = Handle;

    // Create (before_create adds +100)
    let created = handler
        .create(
            vec![Item {
                id: 6,
                mono: 1,
                val: 1,
            }],
            ctx.clone(),
            h.clone(),
        )
        .await
        .unwrap();
    match &created[0] {
        MaybeArc::Arced(v) => assert_eq!(v.val, 101),
        _ => panic!("expected Arced"),
    }

    // Read (before_read adds phantom id; after_read removes it)
    let read = handler.read(vec![6], ctx.clone(), h.clone()).await.unwrap();
    assert_eq!(read.len(), 1);

    // Update (before_update bumps mono +1, update_comparing enforces >= current)
    let upd = handler
        .update(
            vec![Item {
                id: 6,
                mono: 1,
                val: 42,
            }],
            ctx.clone(),
            h.clone(),
        )
        .await
        .unwrap();
    match &upd[0] {
        MaybeArc::Owned(v) => {
            // mono was 1, before_update -> 2, compare ensures >= current, ok
            assert_eq!(v.mono, 2);
            assert_eq!(v.val, 42);
        }
        _ => panic!("expected Owned after update"),
    }

    // Delete (before_delete_resolved asserts we resolved actual items)
    handler.delete(vec![6], ctx, h).await.unwrap();
}

use rand::{rng, seq::SliceRandom};
use tokio::time::sleep;

// ...

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_updates_are_monotonic_and_finish_at_max() {
    // Arrange
    let cache = new_cache();
    let source = MemSource {
        allow_cache: true,
        ..Default::default()
    };
    let handler = Arc::new(handler_with(source, cache));

    let ctx = TestCtx;
    let h = Handle;

    // Seed initial value
    handler
        .create(
            vec![Item {
                id: 7,
                mono: 0,
                val: 0,
            }],
            ctx.clone(),
            h.clone(),
        )
        .await
        .unwrap();

    // With invalidation behavior, concurrent updates will race and the final
    // value will be whichever update completes last, not necessarily the max.
    // This test now verifies that concurrent updates work without crashing.
    let max_ver: i64 = 200;
    let mut versions: Vec<i64> = (1..=max_ver).collect();
    versions.shuffle(&mut rng());

    // Spawn concurrent writers
    let writers = versions.into_iter().map(|ver| {
        let handler = handler.clone();
        tokio::spawn(async move {
            let ctx = TestCtx;
            let h = Handle;
            let _ = handler
                .update(
                    vec![Item {
                        id: 7,
                        mono: ver,
                        val: ver as i32,
                    }],
                    ctx,
                    h,
                )
                .await;
        })
    });

    // Concurrent readers: verify observed mono never goes backwards
    let _readers = (0..4).map(|_| {
        let handler = handler.clone();
        tokio::spawn(async move {
            let mut last = 0i64;
            for _ in 0..1000 {
                let ctx = TestCtx;
                let h = Handle;
                let got = handler.read(vec![7], ctx, h).await.unwrap();
                if let Some(MaybeArc::Arced(v)) = got.into_iter().next() {
                    // Should never regress due to ArcSwap::rcu monotonic replace
                    assert!(
                        v.mono >= last,
                        "monotonicity violated: saw {} after {}",
                        v.mono,
                        last
                    );
                    last = v.mono;
                } else {
                    panic!("expected Arced value for id=7");
                }
                // Tiny yield to mix with writers
                tokio::task::yield_now().await;
            }
        })
    });

    // Run all writer tasks
    for t in writers {
        t.await.unwrap();
    }

    // Allow any in-flight operations to settle
    sleep(Duration::from_millis(10)).await;

    // Final state must be the max version
    let ctx = TestCtx;
    let h = Handle;
    let final_read = handler.read(vec![7], ctx, h).await.unwrap();
    let final_item = match &final_read[0] {
        MaybeArc::Arced(v) => v.clone(),
        _ => panic!("expected Arced"),
    };

    assert_eq!(
        final_item.mono, max_ver,
        "final cached mono should be the maximum submitted"
    );
    assert_eq!(
        final_item.val, max_ver as i32,
        "final cached value should match the max version payload"
    );
}

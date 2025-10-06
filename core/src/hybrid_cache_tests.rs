#[cfg(test)]
mod tests {
    use crate::{CrudableMap, HybridCrudableMap, MokaConfig};
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct TestUser {
        id: u64,
        name: String,
        version: u64,
    }

    impl crate::Crudable for TestUser {
        type Pkey = u64;
        type MonoField = u64;

        fn pkey(&self) -> Self::Pkey {
            self.id
        }

        fn mono_field(&self) -> Self::MonoField {
            self.version
        }
    }

    #[tokio::test]
    async fn test_moka_only_cache() {
        let cache = HybridCrudableMap::<TestUser>::moka_only(MokaConfig::default());

        let user = TestUser {
            id: 1,
            name: "Alice".to_string(),
            version: 1,
        };

        // Test insert
        let inserted = cache.insert(vec![user.clone()]).await;
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0].name, "Alice");

        // Test get
        let results = cache.get(&[1, 2]).await;
        assert_eq!(results.len(), 2);
        assert!(results[0].is_some());
        assert!(results[1].is_none());

        // Test invalidate
        cache.invalidate(&[1]).await;
        let results = cache.get(&[1]).await;
        assert!(results[0].is_none());
    }

    #[tokio::test]
    async fn test_monotonic_field_logic() {
        let cache = HybridCrudableMap::<TestUser>::moka_only(MokaConfig::default());

        let user_v1 = TestUser {
            id: 1,
            name: "Alice v1".to_string(),
            version: 1,
        };

        let user_v2 = TestUser {
            id: 1,
            name: "Alice v2".to_string(),
            version: 2,
        };

        let user_v0 = TestUser {
            id: 1,
            name: "Alice v0".to_string(),
            version: 0,
        };

        // Insert v1
        cache.insert(vec![user_v1]).await;

        // Insert v2 (should update because 2 > 1)
        cache.insert(vec![user_v2]).await;

        // Insert v0 (should NOT update because 0 < 2)
        cache.insert(vec![user_v0]).await;

        let results = cache.get(&[1]).await;
        assert!(results[0].is_some());
        let user = results[0].as_ref().unwrap();
        assert_eq!(user.name, "Alice v2");
        assert_eq!(user.version, 2);
    }

    // Note: Redis tests would require a running Redis instance
    // and proper integration test setup
}
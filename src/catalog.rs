//! DuckLake catalog provider implementation

use std::any::Any;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use crate::Result;
use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::parse_object_store_url;
use crate::schema::DuckLakeSchema;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;

/// Trait for providing current time (allows mocking in tests)
trait Clock: Send + Sync + std::fmt::Debug {
    fn now(&self) -> Instant;
}

/// Standard clock using std::time::Instant
#[derive(Debug)]
struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Configuration for snapshot resolution behavior
#[derive(Debug, Clone)]
pub struct SnapshotConfig {
    /// Time-to-live for cached snapshot ID
    /// - Some(Duration::ZERO): Always query for latest snapshot (maximum freshness)
    /// - Some(duration) where duration > 0: Cache snapshot for specified duration
    /// - None: Cache forever (snapshot frozen at catalog creation)
    pub ttl: Option<Duration>,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            // Default to Duration::ZERO for maximum freshness
            ttl: Some(Duration::ZERO),
        }
    }
}

/// Cached snapshot with timestamp
#[derive(Debug, Clone)]
struct SnapshotCache {
    snapshot_id: i64,
    cached_at: Instant,
}

/// DuckLake catalog provider with configurable snapshot resolution
///
/// Connects to a DuckLake catalog database and provides access to schemas and tables.
/// Uses dynamic metadata lookup - schemas are queried on-demand from the catalog database.
/// Supports configurable snapshot resolution with TTL for balancing freshness and performance.
#[derive(Debug)]
pub struct DuckLakeCatalog {
    /// Metadata provider for querying catalog
    provider: Arc<dyn MetadataProvider>,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    /// Catalog base path component for resolving relative schema paths (e.g., /prefix/)
    catalog_path: String,
    /// Configuration for snapshot resolution
    config: SnapshotConfig,
    /// Cached snapshot with timestamp
    cached_snapshot: RwLock<Option<SnapshotCache>>,
    /// Clock provider for time operations (allows mocking in tests)
    clock: Arc<dyn Clock>,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog with default configuration (TTL = 0 for maximum freshness)
    ///
    /// During catalog creation, only fetches data_path from the metadata provider.
    pub fn new(provider: impl MetadataProvider + 'static) -> Result<Self> {
        Self::new_with_config(provider, SnapshotConfig::default())
    }

    /// Create a new DuckLake catalog with custom snapshot configuration
    pub fn new_with_config(
        provider: impl MetadataProvider + 'static,
        config: SnapshotConfig,
    ) -> Result<Self> {
        let provider = Arc::new(provider) as Arc<dyn MetadataProvider>;
        Self::from_arc_provider(provider, config)
    }

    /// Internal constructor that accepts Arc<dyn MetadataProvider>
    fn from_arc_provider(
        provider: Arc<dyn MetadataProvider>,
        config: SnapshotConfig,
    ) -> Result<Self> {
        let data_path = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path)?;

        Ok(Self {
            provider,
            object_store_url: Arc::new(object_store_url),
            catalog_path,
            config,
            cached_snapshot: RwLock::new(None),
            clock: Arc::new(SystemClock),
        })
    }

    /// Internal constructor with injectable clock (for testing)
    #[cfg(test)]
    fn from_arc_provider_with_clock(
        provider: Arc<dyn MetadataProvider>,
        config: SnapshotConfig,
        clock: Arc<dyn Clock>,
    ) -> Result<Self> {
        let data_path = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path)?;

        Ok(Self {
            provider,
            object_store_url: Arc::new(object_store_url),
            catalog_path,
            config,
            cached_snapshot: RwLock::new(None),
            clock,
        })
    }

    /// Helper function for double-checked locking pattern to get or refresh snapshot
    /// Returns cached snapshot if valid according to predicate, otherwise queries fresh snapshot
    fn get_or_refresh_snapshot<F>(&self, is_valid: F) -> Result<i64>
    where
        F: Fn(&SnapshotCache, Instant) -> bool,
    {
        // Check if cache is valid (read lock)
        {
            let cache = self
                .cached_snapshot
                .read()
                .expect("Snapshot cache lock poisoned");
            if let Some(cached) = cache.as_ref()
                && is_valid(cached, self.clock.now())
            {
                return Ok(cached.snapshot_id);
            }
        }

        // Cache invalid or empty, refresh (write lock)
        let mut cache = self
            .cached_snapshot
            .write()
            .expect("Snapshot cache lock poisoned");

        // Re-calculate now for precise timing (time may have elapsed acquiring write lock)
        let now = self.clock.now();

        // Double-check (another thread might have refreshed)
        if let Some(cached) = cache.as_ref()
            && is_valid(cached, now)
        {
            return Ok(cached.snapshot_id);
        }

        // Query fresh snapshot
        let snapshot_id = self
            .provider
            .get_current_snapshot()
            .inspect_err(|e| tracing::error!(error = %e, "Failed to get current snapshot"))?;
        *cache = Some(SnapshotCache {
            snapshot_id,
            cached_at: now,
        });

        Ok(snapshot_id)
    }

    fn get_current_snapshot_id(&self) -> Result<i64> {
        match self.config.ttl {
            // TTL = Duration::ZERO: Always query for fresh snapshot
            Some(ttl) if ttl.is_zero() => self
                .provider
                .get_current_snapshot()
                .inspect_err(|e| tracing::error!(error = %e, "Failed to get current snapshot")),

            // TTL > 0: Use cache if not expired
            Some(ttl) => self.get_or_refresh_snapshot(|cached, now| {
                let age = now.duration_since(cached.cached_at);
                age < ttl
            }),

            // TTL = None: Cache forever
            None => self.get_or_refresh_snapshot(|_cached, _now| true),
        }
    }
}

impl CatalogProvider for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let snapshot_id = match self.get_current_snapshot_id() {
            Ok(id) => id,
            Err(_) => return Vec::new(),
        };

        // Query database with snapshot_id
        self.provider
            .list_schemas(snapshot_id)
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.schema_name)
            .collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let snapshot_id = match self.get_current_snapshot_id() {
            Ok(id) => id,
            Err(_) => return None,
        };

        // Query database with snapshot_id
        match self.provider.get_schema_by_name(name, snapshot_id) {
            Ok(Some(meta)) => {
                // Resolve schema path hierarchically
                let schema_path = if meta.path_is_relative {
                    // Schema path is relative to catalog path
                    format!("{}{}", self.catalog_path, meta.path)
                } else {
                    // Schema path is absolute
                    meta.path
                };

                // Pass snapshot_id to schema
                Some(Arc::new(DuckLakeSchema::new(
                    meta.schema_id,
                    meta.schema_name,
                    Arc::clone(&self.provider),
                    snapshot_id, // Propagate snapshot_id
                    self.object_store_url.clone(),
                    schema_path,
                )) as Arc<dyn SchemaProvider>)
            },
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_provider::{
        DuckLakeTableColumn, DuckLakeTableFile, SchemaMetadata, TableMetadata,
    };
    use std::cell::RefCell;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicI64, Ordering};
    use std::thread;

    /// Mock clock for deterministic time testing
    #[derive(Debug)]
    struct MockClock {
        current_time: Mutex<RefCell<Instant>>,
    }

    impl MockClock {
        fn new() -> Self {
            Self {
                current_time: Mutex::new(RefCell::new(Instant::now())),
            }
        }

        fn advance(&self, duration: Duration) {
            let guard = self.current_time.lock().unwrap();
            let mut time = guard.borrow_mut();
            *time += duration;
        }
    }

    impl Clock for MockClock {
        fn now(&self) -> Instant {
            let guard = self.current_time.lock().unwrap();
            *guard.borrow()
        }
    }

    /// Mock metadata provider for testing snapshot resolution
    #[derive(Debug)]
    struct MockMetadataProvider {
        snapshot_counter: AtomicI64,
    }

    impl MockMetadataProvider {
        fn new(initial_snapshot: i64) -> Self {
            Self {
                snapshot_counter: AtomicI64::new(initial_snapshot),
            }
        }

        fn increment_snapshot(&self) {
            self.snapshot_counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    impl crate::metadata_provider::MetadataProvider for MockMetadataProvider {
        fn get_current_snapshot(&self) -> crate::Result<i64> {
            Ok(self.snapshot_counter.load(Ordering::SeqCst))
        }

        fn get_data_path(&self) -> crate::Result<String> {
            Ok("file:///tmp/test".to_string())
        }

        fn list_schemas(&self, _snapshot_id: i64) -> crate::Result<Vec<SchemaMetadata>> {
            Ok(vec![])
        }

        fn list_tables(
            &self,
            _schema_id: i64,
            _snapshot_id: i64,
        ) -> crate::Result<Vec<TableMetadata>> {
            Ok(vec![])
        }

        fn get_table_structure(&self, _table_id: i64) -> crate::Result<Vec<DuckLakeTableColumn>> {
            Ok(vec![])
        }

        fn get_table_files_for_select(
            &self,
            _table_id: i64,
            _snapshot_id: i64,
        ) -> crate::Result<Vec<DuckLakeTableFile>> {
            Ok(vec![])
        }

        fn get_schema_by_name(
            &self,
            _name: &str,
            _snapshot_id: i64,
        ) -> crate::Result<Option<SchemaMetadata>> {
            Ok(None)
        }

        fn get_table_by_name(
            &self,
            _schema_id: i64,
            _name: &str,
            _snapshot_id: i64,
        ) -> crate::Result<Option<TableMetadata>> {
            Ok(None)
        }

        fn table_exists(
            &self,
            _schema_id: i64,
            _name: &str,
            _snapshot_id: i64,
        ) -> crate::Result<bool> {
            Ok(false)
        }
    }

    #[test]
    fn test_snapshot_config_default() {
        let config = SnapshotConfig::default();
        assert_eq!(config.ttl, Some(Duration::ZERO));
    }

    #[test]
    fn test_ttl_zero_always_queries_fresh() {
        let provider = Arc::new(MockMetadataProvider::new(100));
        let provider_trait: Arc<dyn MetadataProvider> = provider.clone();
        let config = SnapshotConfig {
            ttl: Some(Duration::ZERO),
        };
        let catalog = DuckLakeCatalog::from_arc_provider(provider_trait, config).unwrap();

        // First query should return snapshot 100
        let snapshot1 = catalog.get_current_snapshot_id().unwrap();
        assert_eq!(snapshot1, 100);

        // Increment snapshot externally
        provider.increment_snapshot();

        // Second query should see new snapshot (no caching)
        let snapshot2 = catalog.get_current_snapshot_id().unwrap();
        assert_eq!(snapshot2, 101);
    }

    #[test]
    fn test_ttl_none_caches_forever() {
        let provider = Arc::new(MockMetadataProvider::new(100));
        let provider_trait: Arc<dyn MetadataProvider> = provider.clone();
        let config = SnapshotConfig {
            ttl: None,
        };
        let catalog = DuckLakeCatalog::from_arc_provider(provider_trait, config).unwrap();

        // First query caches snapshot 100
        let snapshot1 = catalog.get_current_snapshot_id().unwrap();
        assert_eq!(snapshot1, 100);

        // Increment snapshot externally
        provider.increment_snapshot();

        // Second query should still return cached value
        let snapshot2 = catalog.get_current_snapshot_id().unwrap();
        assert_eq!(snapshot2, 100);
    }

    #[test]
    fn test_ttl_with_duration() {
        let provider = Arc::new(MockMetadataProvider::new(100));
        let provider_trait: Arc<dyn MetadataProvider> = provider.clone();
        let mock_clock = Arc::new(MockClock::new());
        let config = SnapshotConfig {
            ttl: Some(Duration::from_secs(1)),
        };
        let catalog = DuckLakeCatalog::from_arc_provider_with_clock(
            provider_trait,
            config,
            mock_clock.clone(),
        )
        .unwrap();

        // First query caches snapshot 100
        let snapshot1 = catalog.get_current_snapshot_id().unwrap();
        assert_eq!(snapshot1, 100);

        // Increment snapshot externally
        provider.increment_snapshot();

        // Immediate second query uses cache
        let snapshot2 = catalog.get_current_snapshot_id().unwrap();
        assert_eq!(snapshot2, 100);

        // Advance time past TTL
        mock_clock.advance(Duration::from_secs(2));

        // Query after expiration fetches fresh snapshot
        let snapshot3 = catalog.get_current_snapshot_id().unwrap();
        assert_eq!(snapshot3, 101);
    }

    #[test]
    fn test_concurrent_snapshot_access() {
        let provider = MockMetadataProvider::new(100);
        let config = SnapshotConfig {
            ttl: Some(Duration::from_secs(1)),
        };
        let catalog = Arc::new(DuckLakeCatalog::new_with_config(provider, config).unwrap());

        let mut handles = vec![];

        // Spawn multiple threads accessing snapshot concurrently
        for _ in 0..10 {
            let catalog_clone = Arc::clone(&catalog);
            let handle = thread::spawn(move || {
                for _ in 0..10 {
                    let _ = catalog_clone.get_current_snapshot_id();
                }
            });
            handles.push(handle);
        }

        // Wait for all threads to complete
        for handle in handles {
            handle.join().unwrap();
        }

        // Test passes if no panics occurred (validates RwLock safety)
    }
}

//! Path resolution utilities for DuckLake
//!
//! Provides utilities for parsing object store URLs and resolving hierarchical paths
//! in the DuckLake catalog structure (catalog -> schema -> table -> file).

use crate::{DuckLakeError, Result};
use datafusion::datasource::object_store::ObjectStoreUrl;
use std::sync::Arc;

/// Parses a data path into an ObjectStoreUrl and key path
///
/// Supports three formats:
/// - S3 URLs: `s3://bucket/prefix/path`
/// - File URLs: `file:///absolute/path`
/// - Local paths: `/absolute/path` or `relative/path`
///
/// # Arguments
/// * `data_path` - The path to parse (may be S3, file://, or local path)
///
/// # Returns
/// A tuple of (ObjectStoreUrl, key_path) where:
/// - `ObjectStoreUrl` is the base URL for the object store (e.g., `s3://bucket/` or `file:///`)
/// - `key_path` is the path component (e.g., `/prefix/path`)
///
/// # Errors
/// Returns error if:
/// - URL parsing fails
/// - S3 URL is missing bucket name
/// - Local path cannot be canonicalized (doesn't exist or permission denied)
///
/// # Example
/// ```no_run
/// # use datafusion_ducklake::path_resolver::parse_object_store_url;
/// let (url, path) = parse_object_store_url("s3://my-bucket/data/tables")?;
/// // url = ObjectStoreUrl("s3://my-bucket/")
/// // path = "/data/tables"
/// # Ok::<(), datafusion_ducklake::DuckLakeError>(())
/// ```
pub fn parse_object_store_url(data_path: &str) -> Result<(ObjectStoreUrl, String)> {
    if data_path.starts_with("s3://") {
        parse_s3_url(data_path)
    } else if data_path.starts_with("file://") {
        parse_file_url(data_path)
    } else {
        parse_local_path(data_path)
    }
}

/// Parse an S3 URL into ObjectStoreUrl and key path
fn parse_s3_url(data_path: &str) -> Result<(ObjectStoreUrl, String)> {
    let url = url::Url::parse(data_path).map_err(|e| {
        DuckLakeError::InvalidConfig(format!("Failed to parse S3 URL '{}': {}", data_path, e))
    })?;

    let bucket = url.host_str().ok_or_else(|| {
        DuckLakeError::InvalidConfig(format!("S3 URL missing bucket: {}", data_path))
    })?;

    let object_store_url = ObjectStoreUrl::parse(format!("s3://{}/", bucket)).map_err(|e| {
        DuckLakeError::InvalidConfig(format!("Failed to create ObjectStoreUrl: {}", e))
    })?;

    Ok((object_store_url, url.path().to_owned()))
}

/// Parse a file:// URL into ObjectStoreUrl and key path
fn parse_file_url(data_path: &str) -> Result<(ObjectStoreUrl, String)> {
    let url = url::Url::parse(data_path).map_err(|e| {
        DuckLakeError::InvalidConfig(format!(
            "Failed to parse file URL '{}': {}",
            data_path, e
        ))
    })?;

    let object_store_url = ObjectStoreUrl::parse("file:///").map_err(|e| {
        DuckLakeError::InvalidConfig(format!("Failed to create ObjectStoreUrl: {}", e))
    })?;

    Ok((object_store_url, url.path().to_owned()))
}

/// Parse a local filesystem path into ObjectStoreUrl and key path
fn parse_local_path(data_path: &str) -> Result<(ObjectStoreUrl, String)> {
    let has_trailing_slash = data_path.ends_with('/') || data_path.ends_with('\\');

    let absolute_path = std::path::PathBuf::from(data_path)
        .canonicalize()
        .map_err(|e| {
            DuckLakeError::InvalidConfig(format!(
                "Failed to resolve path '{}': {}",
                data_path, e
            ))
        })?;

    let object_store_url = ObjectStoreUrl::parse("file:///").map_err(|e| {
        DuckLakeError::InvalidConfig(format!("Failed to create ObjectStoreUrl: {}", e))
    })?;

    let mut path_str = absolute_path.to_string_lossy().to_string();
    // Preserve trailing slash if original had one
    if has_trailing_slash && !path_str.ends_with('/') && !path_str.ends_with('\\') {
        path_str.push('/');
    }

    Ok((object_store_url, path_str))
}

/// Resolves a path that may be relative or absolute
///
/// If the path is relative, it's joined with the base path.
/// If the path is absolute, it's returned as-is.
///
/// # Arguments
/// * `base_path` - The base path to resolve against
/// * `path` - The path to resolve (may be relative or absolute)
/// * `is_relative` - Whether the path is relative to the base path
///
/// # Returns
/// The resolved absolute path
///
/// # Example
/// ```
/// # use datafusion_ducklake::path_resolver::resolve_path;
/// let resolved = resolve_path("/data/schema1/", "table1/data.parquet", true);
/// assert_eq!(resolved, "/data/schema1/table1/data.parquet");
///
/// let absolute = resolve_path("/data/schema1/", "/other/path/data.parquet", false);
/// assert_eq!(absolute, "/other/path/data.parquet");
/// ```
pub fn resolve_path(base_path: &str, path: &str, is_relative: bool) -> String {
    if is_relative {
        join_paths(base_path, path)
    } else {
        path.to_string()
    }
}

/// Joins a base path with a relative path, ensuring proper separators
///
/// Handles trailing slashes correctly to avoid double slashes or missing separators.
///
/// # Arguments
/// * `base_path` - The base path (should be absolute)
/// * `relative_path` - The relative path to append
///
/// # Returns
/// The joined path with proper separators
///
/// # Example
/// ```
/// # use datafusion_ducklake::path_resolver::join_paths;
/// assert_eq!(join_paths("/data/", "table/file.parquet"), "/data/table/file.parquet");
/// assert_eq!(join_paths("/data", "table/file.parquet"), "/data/table/file.parquet");
/// assert_eq!(join_paths("/data/", "/absolute"), "/data//absolute"); // preserves if relative starts with /
/// ```
pub fn join_paths(base_path: &str, relative_path: &str) -> String {
    if base_path.ends_with('/') || base_path.ends_with('\\') {
        format!("{}{}", base_path, relative_path)
    } else {
        format!("{}/{}", base_path, relative_path)
    }
}

/// Path resolver for hierarchical DuckLake paths
///
/// Maintains a base URL and base path for resolving relative paths in the
/// catalog hierarchy (catalog -> schema -> table -> file).
#[derive(Debug, Clone)]
pub struct PathResolver {
    /// Base object store URL (e.g., s3://bucket/ or file:///)
    base_url: Arc<ObjectStoreUrl>,
    /// Base path for resolving relative paths (e.g., /prefix/schema/table/)
    base_path: String,
}

impl PathResolver {
    /// Creates a new PathResolver with a base URL and path
    ///
    /// # Arguments
    /// * `base_url` - The object store URL (e.g., `s3://bucket/` or `file:///`)
    /// * `base_path` - The base path for resolving relative paths
    ///
    /// # Example
    /// ```no_run
    /// # use datafusion_ducklake::path_resolver::PathResolver;
    /// # use datafusion::datasource::object_store::ObjectStoreUrl;
    /// # use std::sync::Arc;
    /// let url = ObjectStoreUrl::parse("s3://my-bucket/")?;
    /// let resolver = PathResolver::new(Arc::new(url), "/data/schema1/".to_string());
    /// # Ok::<(), datafusion::error::DataFusionError>(())
    /// ```
    pub fn new(base_url: Arc<ObjectStoreUrl>, base_path: String) -> Self {
        Self {
            base_url,
            base_path,
        }
    }

    /// Resolves a path that may be relative or absolute
    ///
    /// # Arguments
    /// * `path` - The path to resolve
    /// * `is_relative` - Whether the path is relative to the base path
    ///
    /// # Returns
    /// The resolved absolute path
    pub fn resolve(&self, path: &str, is_relative: bool) -> String {
        resolve_path(&self.base_path, path, is_relative)
    }

    /// Creates a child resolver with a new base path
    ///
    /// This is useful for creating resolvers at each level of the hierarchy:
    /// catalog resolver -> schema resolver -> table resolver
    ///
    /// # Arguments
    /// * `path` - The path to resolve (may be relative or absolute)
    /// * `is_relative` - Whether the path is relative to the current base path
    ///
    /// # Returns
    /// A new PathResolver with the resolved path as its base path
    ///
    /// # Example
    /// ```no_run
    /// # use datafusion_ducklake::path_resolver::PathResolver;
    /// # use datafusion::datasource::object_store::ObjectStoreUrl;
    /// # use std::sync::Arc;
    /// let catalog_resolver = PathResolver::new(
    ///     Arc::new(ObjectStoreUrl::parse("s3://bucket/")?),
    ///     "/data/".to_string()
    /// );
    /// let schema_resolver = catalog_resolver.child_resolver("schema1/", true);
    /// // schema_resolver now has base_path = "/data/schema1/"
    /// # Ok::<(), datafusion::error::DataFusionError>(())
    /// ```
    pub fn child_resolver(&self, path: &str, is_relative: bool) -> Self {
        let resolved = self.resolve(path, is_relative);
        Self::new(self.base_url.clone(), resolved)
    }

    /// Returns the base object store URL
    pub fn base_url(&self) -> &Arc<ObjectStoreUrl> {
        &self.base_url
    }

    /// Returns the base path
    pub fn base_path(&self) -> &str {
        &self.base_path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_s3_url() {
        let (url, path) = parse_object_store_url("s3://bucket/prefix/data").unwrap();
        assert_eq!(path, "/prefix/data");
        assert_eq!(
            url,
            ObjectStoreUrl::parse("s3://bucket/").unwrap()
        );
    }

    #[test]
    fn test_parse_s3_url_with_trailing_slash() {
        let (url, path) = parse_object_store_url("s3://bucket/prefix/").unwrap();
        assert_eq!(path, "/prefix/");
        assert_eq!(
            url,
            ObjectStoreUrl::parse("s3://bucket/").unwrap()
        );
    }

    #[test]
    fn test_parse_s3_url_no_prefix() {
        let (url, path) = parse_object_store_url("s3://bucket/").unwrap();
        assert_eq!(path, "/");
        assert_eq!(
            url,
            ObjectStoreUrl::parse("s3://bucket/").unwrap()
        );
    }

    #[test]
    fn test_parse_file_url() {
        let (url, path) = parse_object_store_url("file:///tmp/data").unwrap();
        assert_eq!(path, "/tmp/data");
        assert_eq!(url, ObjectStoreUrl::parse("file:///").unwrap());
    }

    #[test]
    fn test_parse_file_url_with_trailing_slash() {
        let (url, path) = parse_object_store_url("file:///tmp/data/").unwrap();
        assert_eq!(path, "/tmp/data/");
        assert_eq!(url, ObjectStoreUrl::parse("file:///").unwrap());
    }

    #[test]
    fn test_parse_s3_url_missing_bucket() {
        let result = parse_object_store_url("s3:///path");
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("S3 URL missing bucket"));
    }

    #[test]
    fn test_resolve_path_relative() {
        let resolved = resolve_path("/data/schema1/", "table1/", true);
        assert_eq!(resolved, "/data/schema1/table1/");
    }

    #[test]
    fn test_resolve_path_relative_no_trailing_slash() {
        let resolved = resolve_path("/data/schema1", "table1/", true);
        assert_eq!(resolved, "/data/schema1/table1/");
    }

    #[test]
    fn test_resolve_path_absolute() {
        let resolved = resolve_path("/data/schema1/", "/other/path/", false);
        assert_eq!(resolved, "/other/path/");
    }

    #[test]
    fn test_join_paths_with_trailing_slash() {
        assert_eq!(
            join_paths("/data/", "table/file.parquet"),
            "/data/table/file.parquet"
        );
    }

    #[test]
    fn test_join_paths_without_trailing_slash() {
        assert_eq!(
            join_paths("/data", "table/file.parquet"),
            "/data/table/file.parquet"
        );
    }

    #[test]
    fn test_join_paths_with_backslash() {
        assert_eq!(
            join_paths("C:\\data\\", "table\\file.parquet"),
            "C:\\data\\table\\file.parquet"
        );
    }

    #[test]
    fn test_path_resolver_resolve() {
        let resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/data/schema1/".to_string(),
        );

        let resolved = resolver.resolve("table1/", true);
        assert_eq!(resolved, "/data/schema1/table1/");

        let absolute = resolver.resolve("/other/path/", false);
        assert_eq!(absolute, "/other/path/");
    }

    #[test]
    fn test_path_resolver_child_resolver() {
        let catalog_resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/data/".to_string(),
        );

        let schema_resolver = catalog_resolver.child_resolver("schema1/", true);
        assert_eq!(schema_resolver.base_path(), "/data/schema1/");
        assert_eq!(
            *schema_resolver.base_url(),
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap())
        );

        let table_resolver = schema_resolver.child_resolver("table1/", true);
        assert_eq!(table_resolver.base_path(), "/data/schema1/table1/");
    }

    #[test]
    fn test_path_resolver_child_resolver_absolute() {
        let catalog_resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/data/".to_string(),
        );

        let schema_resolver = catalog_resolver.child_resolver("/other/schema/", false);
        assert_eq!(schema_resolver.base_path(), "/other/schema/");
    }

    // Additional edge case tests

    #[test]
    fn test_parse_s3_url_with_deep_prefix() {
        let (url, path) = parse_object_store_url("s3://my-bucket/level1/level2/level3/data").unwrap();
        assert_eq!(path, "/level1/level2/level3/data");
        assert_eq!(url, ObjectStoreUrl::parse("s3://my-bucket/").unwrap());
    }

    #[test]
    fn test_parse_s3_url_bucket_only() {
        let (url, path) = parse_object_store_url("s3://my-bucket").unwrap();
        assert_eq!(path, "");
        assert_eq!(url, ObjectStoreUrl::parse("s3://my-bucket/").unwrap());
    }

    #[test]
    fn test_parse_s3_url_with_hyphens_and_dots() {
        let (url, path) = parse_object_store_url("s3://my-bucket.name-123/data").unwrap();
        assert_eq!(path, "/data");
        assert_eq!(url, ObjectStoreUrl::parse("s3://my-bucket.name-123/").unwrap());
    }

    #[test]
    fn test_parse_invalid_s3_url() {
        let result = parse_object_store_url("s3://");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_file_url_root() {
        let (url, path) = parse_object_store_url("file:///").unwrap();
        assert_eq!(path, "/");
        assert_eq!(url, ObjectStoreUrl::parse("file:///").unwrap());
    }

    #[test]
    fn test_parse_file_url_with_host() {
        // file://host/path is technically valid (networked file path)
        let (url, path) = parse_object_store_url("file://localhost/tmp/data").unwrap();
        assert_eq!(path, "/tmp/data");
        assert_eq!(url, ObjectStoreUrl::parse("file:///").unwrap());
    }

    #[test]
    fn test_parse_local_path_nonexistent() {
        let result = parse_object_store_url("/nonexistent/path/that/does/not/exist");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Failed to resolve path"));
    }

    #[test]
    fn test_resolve_path_empty_relative() {
        let resolved = resolve_path("/data/schema1/", "", true);
        assert_eq!(resolved, "/data/schema1/");
    }

    #[test]
    fn test_resolve_path_empty_absolute() {
        let resolved = resolve_path("/data/schema1/", "", false);
        assert_eq!(resolved, "");
    }

    #[test]
    fn test_join_paths_empty_relative() {
        assert_eq!(join_paths("/data/", ""), "/data/");
        assert_eq!(join_paths("/data", ""), "/data/");
    }

    #[test]
    fn test_join_paths_nested_relative() {
        assert_eq!(
            join_paths("/data/", "schema1/table1/file.parquet"),
            "/data/schema1/table1/file.parquet"
        );
    }

    #[test]
    fn test_join_paths_windows_style() {
        assert_eq!(
            join_paths("C:\\data", "schema1\\table1"),
            "C:\\data/schema1\\table1"
        );
    }

    #[test]
    fn test_path_resolver_multiple_levels() {
        let catalog_resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/data/".to_string(),
        );

        let schema_resolver = catalog_resolver.child_resolver("schema1/", true);
        let table_resolver = schema_resolver.child_resolver("table1/", true);
        let file_path = table_resolver.resolve("file.parquet", true);

        assert_eq!(file_path, "/data/schema1/table1/file.parquet");
    }

    #[test]
    fn test_path_resolver_clone() {
        let resolver1 = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/data/".to_string(),
        );

        let resolver2 = resolver1.clone();
        assert_eq!(resolver1.base_path(), resolver2.base_path());
        assert_eq!(*resolver1.base_url(), *resolver2.base_url());
    }

    #[test]
    fn test_path_resolver_base_url_accessor() {
        let url = Arc::new(ObjectStoreUrl::parse("s3://test-bucket/").unwrap());
        let resolver = PathResolver::new(url.clone(), "/data/".to_string());

        assert_eq!(*resolver.base_url(), url);
    }

    #[test]
    fn test_path_resolver_base_path_accessor() {
        let resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/my/base/path/".to_string(),
        );

        assert_eq!(resolver.base_path(), "/my/base/path/");
    }

    #[test]
    fn test_resolve_path_with_special_chars() {
        let resolved = resolve_path("/data/", "table-123_test.v2/", true);
        assert_eq!(resolved, "/data/table-123_test.v2/");
    }

    #[test]
    fn test_join_paths_double_slash_in_relative() {
        // If relative path starts with /, we preserve it (though it's unusual)
        assert_eq!(join_paths("/data/", "/absolute"), "/data//absolute");
    }

    #[test]
    fn test_path_resolver_mixed_relative_absolute() {
        let catalog_resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/catalog/".to_string(),
        );

        // First child is relative
        let schema_resolver = catalog_resolver.child_resolver("schema1/", true);
        assert_eq!(schema_resolver.base_path(), "/catalog/schema1/");

        // Second child is absolute (overrides)
        let table_resolver = schema_resolver.child_resolver("/absolute/table/", false);
        assert_eq!(table_resolver.base_path(), "/absolute/table/");

        // Third child is relative to the absolute path
        let file_resolver = table_resolver.child_resolver("subdir/", true);
        assert_eq!(file_resolver.base_path(), "/absolute/table/subdir/");
    }
}

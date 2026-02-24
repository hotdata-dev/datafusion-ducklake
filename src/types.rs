//! Type mapping from DuckLake types to Arrow types

use std::collections::HashMap;

#[cfg(test)]
use std::sync::Arc;

use crate::metadata_provider::DuckLakeTableColumn;
use crate::{DuckLakeError, Result};
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use parquet::file::metadata::ParquetMetaData;

/// Convert a DuckLake type string to an Arrow DataType
pub fn ducklake_to_arrow_type(ducklake_type: &str) -> Result<DataType> {
    // Normalize type string (lowercase, remove whitespace)
    let normalized = ducklake_type.trim().to_lowercase();

    if normalized.is_empty() {
        return Err(DuckLakeError::UnsupportedType(
            "empty type string is not a valid DuckLake type".to_string(),
        ));
    }

    // Handle parameterized types first
    if let Some(decimal_params) = parse_decimal(&normalized) {
        return Ok(decimal_params);
    }

    // Handle basic types
    match normalized.as_str() {
        // Boolean
        "boolean" | "bool" => Ok(DataType::Boolean),

        // Integers
        "int8" | "tinyint" => Ok(DataType::Int8),
        "int16" | "smallint" => Ok(DataType::Int16),
        "int32" | "int" | "integer" => Ok(DataType::Int32),
        "int64" | "bigint" | "long" => Ok(DataType::Int64),
        "uint8" | "utinyint" => Ok(DataType::UInt8),
        "uint16" | "usmallint" => Ok(DataType::UInt16),
        "uint32" | "uint" | "uinteger" => Ok(DataType::UInt32),
        "uint64" | "ubigint" => Ok(DataType::UInt64),

        // Floating point
        "float32" | "float" | "real" => Ok(DataType::Float32),
        "float64" | "double" => Ok(DataType::Float64),

        // Temporal types
        "time" => Ok(DataType::Time64(TimeUnit::Microsecond)),
        "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "timestamptz" | "timestamp with time zone" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        "timestamp_s" => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        "timestamp_ms" => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        "timestamp_ns" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        "interval" => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),

        // String types
        "varchar" | "text" | "string" => Ok(DataType::Utf8),
        "json" => Ok(DataType::Utf8), // JSON stored as UTF8 string

        // Binary types
        "blob" | "binary" | "bytea" => Ok(DataType::Binary),
        "uuid" => Ok(DataType::FixedSizeBinary(16)),

        // Geometry types (stored as binary WKB format)
        "point" | "linestring" | "polygon" | "multipoint" | "multilinestring" | "multipolygon"
        | "geometrycollection" | "linestring z" | "geometry" => Ok(DataType::Binary),

        // Time with timezone - not directly supported, use string
        "timetz" | "time with time zone" => Ok(DataType::Utf8),

        _ => {
            // Check for complex types (list, struct, map)
            if normalized.starts_with("list") || normalized.starts_with("array") {
                Err(DuckLakeError::UnsupportedType(format!(
                    "Complex type '{}' not yet supported. Please open an issue at https://github.com/hotdata-dev/datafusion-ducklake if you need this feature.",
                    ducklake_type
                )))
            } else if normalized.starts_with("struct") {
                Err(DuckLakeError::UnsupportedType(format!(
                    "Struct type '{}' not yet supported. Please open an issue at https://github.com/hotdata-dev/datafusion-ducklake if you need this feature.",
                    ducklake_type
                )))
            } else if normalized.starts_with("map") {
                Err(DuckLakeError::UnsupportedType(format!(
                    "Map type '{}' not yet supported. Please open an issue at https://github.com/hotdata-dev/datafusion-ducklake if you need this feature.",
                    ducklake_type
                )))
            } else {
                Err(DuckLakeError::UnsupportedType(ducklake_type.to_string()))
            }
        },
    }
}

/// Convert an Arrow DataType to a DuckLake type string
///
/// This is the reverse of `ducklake_to_arrow_type()`.
pub fn arrow_to_ducklake_type(arrow_type: &DataType) -> Result<String> {
    match arrow_type {
        // Boolean
        DataType::Boolean => Ok("boolean".to_string()),

        // Integers
        DataType::Int8 => Ok("int8".to_string()),
        DataType::Int16 => Ok("int16".to_string()),
        DataType::Int32 => Ok("int32".to_string()),
        DataType::Int64 => Ok("int64".to_string()),
        DataType::UInt8 => Ok("uint8".to_string()),
        DataType::UInt16 => Ok("uint16".to_string()),
        DataType::UInt32 => Ok("uint32".to_string()),
        DataType::UInt64 => Ok("uint64".to_string()),

        // Floating point
        DataType::Float32 => Ok("float32".to_string()),
        DataType::Float64 => Ok("float64".to_string()),

        // Temporal types
        DataType::Date32 | DataType::Date64 => Ok("date".to_string()),
        DataType::Time32(_) | DataType::Time64(_) => Ok("time".to_string()),
        DataType::Timestamp(TimeUnit::Second, None) => Ok("timestamp_s".to_string()),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok("timestamp_ms".to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok("timestamp".to_string()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok("timestamp_ns".to_string()),
        DataType::Timestamp(_, Some(_)) => Ok("timestamptz".to_string()),
        DataType::Interval(_) => Ok("interval".to_string()),

        // String types
        DataType::Utf8 | DataType::LargeUtf8 => Ok("varchar".to_string()),

        // Binary types
        DataType::Binary | DataType::LargeBinary => Ok("blob".to_string()),
        DataType::FixedSizeBinary(16) => Ok("uuid".to_string()),
        DataType::FixedSizeBinary(_) => Ok("blob".to_string()),

        // Decimal types
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Ok(format!("decimal({}, {})", precision, scale))
        },

        // Null type - map to varchar as there's no direct equivalent
        DataType::Null => Ok("varchar".to_string()),

        // Complex types - not yet supported for writing
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            Err(DuckLakeError::UnsupportedType(format!(
                "List type '{}' not yet supported for writing",
                arrow_type
            )))
        },
        DataType::Struct(_) => Err(DuckLakeError::UnsupportedType(format!(
            "Struct type '{}' not yet supported for writing",
            arrow_type
        ))),
        DataType::Map(_, _) => Err(DuckLakeError::UnsupportedType(format!(
            "Map type '{}' not yet supported for writing",
            arrow_type
        ))),

        // Other unsupported types
        other => Err(DuckLakeError::UnsupportedType(format!(
            "Arrow type '{}' has no DuckLake equivalent",
            other
        ))),
    }
}

/// Parse decimal type with precision and scale
/// Format: "decimal(precision, scale)" or "decimal(precision)"
fn parse_decimal(type_str: &str) -> Option<DataType> {
    if !type_str.starts_with("decimal") && !type_str.starts_with("numeric") {
        return None;
    }

    // Extract parameters from parentheses
    let start = type_str.find('(')?;
    let end = type_str.find(')')?;
    let params = &type_str[start + 1..end];

    let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();

    match parts.len() {
        1 => {
            // decimal(precision) with scale=0
            let precision: u8 = parts[0].parse().ok()?;
            Some(DataType::Decimal128(precision, 0))
        },
        2 => {
            // decimal(precision, scale)
            let precision: u8 = parts[0].parse().ok()?;
            let scale: i8 = parts[1].parse().ok()?;

            // Use Decimal256 for high precision
            if precision > 38 {
                Some(DataType::Decimal256(precision, scale))
            } else {
                Some(DataType::Decimal128(precision, scale))
            }
        },
        _ => None,
    }
}

/// Build an Arrow schema from a list of DuckLake table columns
pub fn build_arrow_schema(columns: &[DuckLakeTableColumn]) -> Result<Schema> {
    let fields: Result<Vec<Field>> = columns
        .iter()
        .map(|col| {
            let data_type = ducklake_to_arrow_type(&col.column_type)?;
            Ok(Field::new(&col.column_name, data_type, col.is_nullable))
        })
        .collect();

    Ok(Schema::new(fields?))
}

/// Extract field_id to column_name mapping from Parquet metadata.
/// DuckLake column_id == Parquet field_id, enabling column matching after renames.
pub fn extract_parquet_field_ids(metadata: &ParquetMetaData) -> HashMap<i32, String> {
    let schema_descr = metadata.file_metadata().schema_descr();
    let mut field_id_map = HashMap::new();

    for i in 0..schema_descr.num_columns() {
        let column = schema_descr.column(i);
        let basic_info = column.self_type().get_basic_info();

        if basic_info.has_id() {
            let field_id = basic_info.id();
            let column_name = column.name().to_string();
            field_id_map.insert(field_id, column_name);
        }
    }

    field_id_map
}

/// Build a schema for reading Parquet files with renamed columns.
/// Returns (read_schema, name_mapping) where read_schema uses original Parquet names
/// and name_mapping maps old->new for columns that were renamed.
pub fn build_read_schema_with_field_id_mapping(
    current_columns: &[DuckLakeTableColumn],
    parquet_field_ids: &HashMap<i32, String>,
) -> Result<(Schema, HashMap<String, String>)> {
    let mut name_mapping: HashMap<String, String> = HashMap::new();

    let fields: Result<Vec<Field>> = current_columns
        .iter()
        .map(|col| {
            let data_type = ducklake_to_arrow_type(&col.column_type)?;
            let field_id = col.column_id as i32;

            let (read_name, needs_rename) =
                if let Some(parquet_name) = parquet_field_ids.get(&field_id) {
                    if parquet_name != &col.column_name {
                        (parquet_name.clone(), true) // Column was renamed
                    } else {
                        (col.column_name.clone(), false)
                    }
                } else {
                    (col.column_name.clone(), false) // No field_id, use current name
                };

            if needs_rename {
                name_mapping.insert(read_name.clone(), col.column_name.clone());
            }

            Ok(Field::new(read_name, data_type, col.is_nullable))
        })
        .collect();

    Ok((Schema::new(fields?), name_mapping))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_read_schema_with_renamed_columns() {
        // Simulate: column was originally named "user_id", now renamed to "userId"
        let current_columns = vec![
            DuckLakeTableColumn {
                column_id: 1,
                column_name: "userId".to_string(), // Current name (renamed)
                column_type: "int32".to_string(),
                is_nullable: true,
            },
            DuckLakeTableColumn {
                column_id: 2,
                column_name: "name".to_string(), // Not renamed
                column_type: "varchar".to_string(),
                is_nullable: true,
            },
        ];

        // Parquet file has original names
        let mut parquet_field_ids = HashMap::new();
        parquet_field_ids.insert(1, "user_id".to_string()); // Original name
        parquet_field_ids.insert(2, "name".to_string()); // Same name

        let (read_schema, name_mapping) =
            build_read_schema_with_field_id_mapping(&current_columns, &parquet_field_ids).unwrap();

        // Read schema should have original Parquet names
        assert_eq!(read_schema.field(0).name(), "user_id");
        assert_eq!(read_schema.field(1).name(), "name");

        // Name mapping should map old name to new name
        assert_eq!(name_mapping.len(), 1);
        assert_eq!(name_mapping.get("user_id"), Some(&"userId".to_string()));
    }

    #[test]
    fn test_build_read_schema_no_rename_needed() {
        let current_columns = vec![DuckLakeTableColumn {
            column_id: 1,
            column_name: "id".to_string(),
            column_type: "int32".to_string(),
            is_nullable: true,
        }];

        let mut parquet_field_ids = HashMap::new();
        parquet_field_ids.insert(1, "id".to_string()); // Same name

        let (read_schema, name_mapping) =
            build_read_schema_with_field_id_mapping(&current_columns, &parquet_field_ids).unwrap();

        assert_eq!(read_schema.field(0).name(), "id");
        assert!(name_mapping.is_empty()); // No rename needed
    }

    #[test]
    fn test_build_read_schema_no_field_ids() {
        // External file without field_ids
        let current_columns = vec![DuckLakeTableColumn {
            column_id: 1,
            column_name: "id".to_string(),
            column_type: "int32".to_string(),
            is_nullable: true,
        }];

        let parquet_field_ids = HashMap::new(); // No field_ids in Parquet

        let (read_schema, name_mapping) =
            build_read_schema_with_field_id_mapping(&current_columns, &parquet_field_ids).unwrap();

        // Falls back to current column name
        assert_eq!(read_schema.field(0).name(), "id");
        assert!(name_mapping.is_empty());
    }

    #[test]
    fn test_basic_types() {
        assert_eq!(
            ducklake_to_arrow_type("boolean").unwrap(),
            DataType::Boolean
        );
        assert_eq!(ducklake_to_arrow_type("int32").unwrap(), DataType::Int32);
        assert_eq!(ducklake_to_arrow_type("int64").unwrap(), DataType::Int64);
        assert_eq!(
            ducklake_to_arrow_type("float64").unwrap(),
            DataType::Float64
        );
        assert_eq!(ducklake_to_arrow_type("varchar").unwrap(), DataType::Utf8);
        assert_eq!(ducklake_to_arrow_type("blob").unwrap(), DataType::Binary);
    }

    #[test]
    fn test_decimal_types() {
        assert_eq!(
            ducklake_to_arrow_type("decimal(10, 2)").unwrap(),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            ducklake_to_arrow_type("decimal(38, 10)").unwrap(),
            DataType::Decimal128(38, 10)
        );
    }

    #[test]
    fn test_temporal_types() {
        assert_eq!(ducklake_to_arrow_type("date").unwrap(), DataType::Date32);
        assert_eq!(
            ducklake_to_arrow_type("timestamp").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn test_unsupported_list_type_errors() {
        // Test list type returns error
        let result = ducklake_to_arrow_type("list<int32>");
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("list<int32>"));
                assert!(msg.contains("not yet supported"));
                assert!(msg.contains("open an issue"));
            },
            _ => panic!("Expected UnsupportedType error for list type"),
        }
    }

    #[test]
    fn test_unsupported_array_type_errors() {
        // Test array type returns error
        let result = ducklake_to_arrow_type("array<varchar>");
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("array<varchar>"));
                assert!(msg.contains("not yet supported"));
            },
            _ => panic!("Expected UnsupportedType error for array type"),
        }
    }

    #[test]
    fn test_unsupported_struct_type_errors() {
        // Test struct type returns error
        let result = ducklake_to_arrow_type("struct<a:int32,b:varchar>");
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("struct<a:int32,b:varchar>"));
                assert!(msg.contains("not yet supported"));
                assert!(msg.contains("open an issue"));
            },
            _ => panic!("Expected UnsupportedType error for struct type"),
        }
    }

    #[test]
    fn test_unsupported_map_type_errors() {
        // Test map type returns error
        let result = ducklake_to_arrow_type("map<varchar,int32>");
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("map<varchar,int32>"));
                assert!(msg.contains("not yet supported"));
                assert!(msg.contains("open an issue"));
            },
            _ => panic!("Expected UnsupportedType error for map type"),
        }
    }

    #[test]
    fn test_nested_complex_types_error() {
        // Test nested complex types return error
        let result = ducklake_to_arrow_type("list<struct<a:int32,b:varchar>>");
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("list<struct<a:int32,b:varchar>>"));
                assert!(msg.contains("not yet supported"));
            },
            _ => panic!("Expected UnsupportedType error for nested complex type"),
        }
    }

    #[test]
    fn test_unknown_type_error() {
        // Test completely unknown types also return error
        let result = ducklake_to_arrow_type("completely_unknown_type");
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert_eq!(msg, "completely_unknown_type");
            },
            _ => panic!("Expected UnsupportedType error for unknown type"),
        }
    }

    #[test]
    fn test_arrow_to_ducklake_basic_types() {
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Boolean).unwrap(),
            "boolean"
        );
        assert_eq!(arrow_to_ducklake_type(&DataType::Int8).unwrap(), "int8");
        assert_eq!(arrow_to_ducklake_type(&DataType::Int16).unwrap(), "int16");
        assert_eq!(arrow_to_ducklake_type(&DataType::Int32).unwrap(), "int32");
        assert_eq!(arrow_to_ducklake_type(&DataType::Int64).unwrap(), "int64");
        assert_eq!(arrow_to_ducklake_type(&DataType::UInt8).unwrap(), "uint8");
        assert_eq!(arrow_to_ducklake_type(&DataType::UInt16).unwrap(), "uint16");
        assert_eq!(arrow_to_ducklake_type(&DataType::UInt32).unwrap(), "uint32");
        assert_eq!(arrow_to_ducklake_type(&DataType::UInt64).unwrap(), "uint64");
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Float32).unwrap(),
            "float32"
        );
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Float64).unwrap(),
            "float64"
        );
        assert_eq!(arrow_to_ducklake_type(&DataType::Utf8).unwrap(), "varchar");
        assert_eq!(arrow_to_ducklake_type(&DataType::Binary).unwrap(), "blob");
    }

    #[test]
    fn test_arrow_to_ducklake_temporal_types() {
        assert_eq!(arrow_to_ducklake_type(&DataType::Date32).unwrap(), "date");
        assert_eq!(arrow_to_ducklake_type(&DataType::Date64).unwrap(), "date");
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Time64(TimeUnit::Microsecond)).unwrap(),
            "time"
        );
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            "timestamp"
        );
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
            .unwrap(),
            "timestamptz"
        );
    }

    #[test]
    fn test_arrow_to_ducklake_decimal() {
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Decimal128(10, 2)).unwrap(),
            "decimal(10, 2)"
        );
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Decimal256(40, 5)).unwrap(),
            "decimal(40, 5)"
        );
    }

    #[test]
    fn test_arrow_to_ducklake_uuid() {
        assert_eq!(
            arrow_to_ducklake_type(&DataType::FixedSizeBinary(16)).unwrap(),
            "uuid"
        );
        // Non-16 byte fixed size binary becomes blob
        assert_eq!(
            arrow_to_ducklake_type(&DataType::FixedSizeBinary(32)).unwrap(),
            "blob"
        );
    }

    #[test]
    fn test_arrow_to_ducklake_roundtrip() {
        // Verify roundtrip: arrow -> ducklake -> arrow for common types
        let test_types = vec![
            DataType::Boolean,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Decimal128(10, 2),
        ];

        for original in test_types {
            let ducklake = arrow_to_ducklake_type(&original).unwrap();
            let back = ducklake_to_arrow_type(&ducklake).unwrap();
            assert_eq!(original, back, "Roundtrip failed for {:?}", original);
        }
    }

    #[test]
    fn test_arrow_to_ducklake_unsupported_list() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let result = arrow_to_ducklake_type(&list_type);
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("List type"));
                assert!(msg.contains("not yet supported"));
            },
            _ => panic!("Expected UnsupportedType error"),
        }
    }

    #[test]
    fn test_arrow_to_ducklake_unsupported_struct() {
        let struct_type = DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into());
        let result = arrow_to_ducklake_type(&struct_type);
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("Struct type"));
                assert!(msg.contains("not yet supported"));
            },
            _ => panic!("Expected UnsupportedType error"),
        }
    }

    #[test]
    fn test_build_schema_with_unsupported_type() {
        // Test that build_arrow_schema propagates complex type errors
        let columns = vec![
            DuckLakeTableColumn {
                column_id: 1,
                column_name: "id".to_string(),
                column_type: "int32".to_string(),
                is_nullable: true,
            },
            DuckLakeTableColumn {
                column_id: 2,
                column_name: "data".to_string(),
                column_type: "list<int32>".to_string(),
                is_nullable: true,
            },
        ];

        let result = build_arrow_schema(&columns);
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("list<int32>"));
            },
            _ => panic!("Expected UnsupportedType error when building schema with complex type"),
        }
    }

    #[test]
    fn test_empty_type_string_error() {
        let result = ducklake_to_arrow_type("");
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("empty"), "Error should mention empty: {}", msg);
            },
            _ => panic!("Expected UnsupportedType error for empty string"),
        }
    }

    #[test]
    fn test_whitespace_only_type_string_error() {
        let result = ducklake_to_arrow_type("   ");
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert!(msg.contains("empty"), "Error should mention empty: {}", msg);
            },
            _ => panic!("Expected UnsupportedType error for whitespace-only string"),
        }
    }
}

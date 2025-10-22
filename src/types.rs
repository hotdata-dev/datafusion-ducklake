//! Type mapping from DuckLake types to Arrow types

use crate::metadata_provider::DuckLakeTableColumn;
use crate::{DuckLakeError, Result};
use arrow::datatypes::{DataType, Field, IntervalUnit, TimeUnit};

/// Convert a DuckLake type string to an Arrow DataType
pub fn ducklake_to_arrow_type(ducklake_type: &str) -> Result<DataType> {
    // Normalize type string (lowercase, remove whitespace)
    let normalized = ducklake_type.trim().to_lowercase();

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
pub fn build_arrow_schema(columns: &[DuckLakeTableColumn]) -> Result<arrow::datatypes::Schema> {
    let fields: Result<Vec<Field>> = columns
        .iter()
        .map(|col| {
            let data_type = ducklake_to_arrow_type(&col.column_type)?;
            Ok(Field::new(&col.column_name, data_type, true)) // nullable=true by default
        })
        .collect();

    Ok(arrow::datatypes::Schema::new(fields?))
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_build_schema_with_unsupported_type() {
        // Test that build_arrow_schema propagates complex type errors
        let columns = vec![
            DuckLakeTableColumn {
                column_id: 1,
                column_name: "id".to_string(),
                column_type: "int32".to_string(),
            },
            DuckLakeTableColumn {
                column_id: 2,
                column_name: "data".to_string(),
                column_type: "list<int32>".to_string(),
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
}

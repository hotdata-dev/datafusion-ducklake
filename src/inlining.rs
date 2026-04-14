//! DuckLake data inlining abstractions and helpers.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder,
    FixedSizeBinaryBuilder, Float32Builder, Float64Builder, Int16Builder, Int32Builder,
    Int64Builder, Int8Builder, LargeBinaryBuilder, LargeStringBuilder, ListBuilder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder,
    TimestampMillisecondBuilder, TimestampNanosecondBuilder, TimestampSecondBuilder,
    UInt16Builder, UInt32Builder, UInt64Builder, UInt8Builder, make_builder,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, TimeUnit};
use arrow::record_batch::RecordBatch;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Timelike, Utc};
use async_trait::async_trait;

use crate::metadata_provider::DuckLakeTableColumn;
use crate::types::{build_arrow_schema, ducklake_to_arrow_type};
#[cfg(feature = "write")]
use crate::metadata_writer::{ColumnDef, WriteMode, WriteResult};
use crate::{DuckLakeError, Result};

/// Metadata entry for an inlined data table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlinedTableRef {
    pub table_name: String,
    pub schema_version: i64,
}

/// Metadata entry for an inlined file deletion row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlinedFileDelete {
    pub file_id: i64,
    pub row_id: i64,
    pub begin_snapshot: i64,
}

/// A single visible row read from an inlined table.
///
/// Values are returned as textual payloads so backend-specific readers can keep
/// storage-specific decoding at the edge while the shared Arrow builders decode
/// them according to the DuckLake column type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InlineRow {
    pub row_id: i64,
    pub values: Vec<Option<String>>,
}

/// Backend-specific read access to DuckLake inline metadata and row storage.
#[async_trait]
pub trait CatalogInliningReader: Send + Sync + Debug {
    /// Load all physical inline tables registered for a logical table.
    async fn get_inlined_table_refs(&self, table_id: i64) -> Result<Vec<InlinedTableRef>>;

    /// Load the logical schema that was active for `table_id` at `schema_version`.
    async fn get_historical_schema(
        &self,
        table_id: i64,
        schema_version: i64,
    ) -> Result<Vec<DuckLakeTableColumn>>;

    /// Read the rows visible at `snapshot_id` from a physical inline table.
    async fn read_visible_inlined_rows(
        &self,
        table_name: &str,
        columns: &[DuckLakeTableColumn],
        snapshot_id: i64,
    ) -> Result<Vec<InlineRow>>;

    /// Load inlined deletions against persisted data files for a logical table.
    async fn get_inlined_file_deletes(
        &self,
        table_id: i64,
        snapshot_id: i64,
    ) -> Result<Vec<InlinedFileDelete>>;
}

/// Backend-specific write access to DuckLake inline storage.
#[cfg(feature = "write")]
pub trait CatalogInliningWriter: Send + Sync + Debug {
    /// Return the current row limit for statement-level inlining.
    fn data_inlining_row_limit(&self) -> Result<usize>;

    /// Return whether the schema can be represented in this backend's inline tables.
    fn supports_inline_columns(&self, columns: &[ColumnDef]) -> Result<bool>;

    /// Write a statement's rows into the inline table path instead of Parquet files.
    fn write_inlined_batches(
        &self,
        schema_name: &str,
        table_name: &str,
        columns: &[ColumnDef],
        batches: &[RecordBatch],
        mode: WriteMode,
    ) -> Result<WriteResult>;
}

/// Reserved column names used by DuckLake inline tables.
pub fn is_reserved_inline_column_name(name: &str) -> bool {
    name.eq_ignore_ascii_case("row_id")
        || name.eq_ignore_ascii_case("begin_snapshot")
        || name.eq_ignore_ascii_case("end_snapshot")
        || name.eq_ignore_ascii_case("_ducklake_internal_snapshot_id")
        || name.eq_ignore_ascii_case("_ducklake_internal_row_id")
}

/// Returns true when any column name would collide with DuckLake inline system columns.
#[cfg(feature = "write")]
pub fn has_reserved_inline_column_names(columns: &[ColumnDef]) -> bool {
    columns
        .iter()
        .any(|column| is_reserved_inline_column_name(column.name()))
}

/// Materialize the currently visible inline data for a table as Arrow batches
/// aligned to the table's current schema.
pub async fn load_visible_inlined_batches(
    reader: &dyn CatalogInliningReader,
    table_id: i64,
    snapshot_id: i64,
    current_columns: &[DuckLakeTableColumn],
) -> Result<Vec<RecordBatch>> {
    let current_schema = Arc::new(build_arrow_schema(current_columns)?);
    let current_index_by_id: HashMap<i64, usize> = current_columns
        .iter()
        .enumerate()
        .map(|(index, column)| (column.column_id, index))
        .collect();

    let mut batches = Vec::new();
    for inline_table in reader.get_inlined_table_refs(table_id).await? {
        let historical_columns = reader
            .get_historical_schema(table_id, inline_table.schema_version)
            .await?;
        let rows = reader
            .read_visible_inlined_rows(&inline_table.table_name, &historical_columns, snapshot_id)
            .await?;

        if rows.is_empty() {
            continue;
        }

        let mut values_by_current_index = vec![vec![None; rows.len()]; current_columns.len()];
        for (historical_index, historical_column) in historical_columns.iter().enumerate() {
            let Some(&current_index) = current_index_by_id.get(&historical_column.column_id) else {
                continue;
            };

            for (row_index, row) in rows.iter().enumerate() {
                values_by_current_index[current_index][row_index] =
                    row.values.get(historical_index).cloned().unwrap_or(None);
            }
        }

        let arrays: Result<Vec<ArrayRef>> = current_columns
            .iter()
            .enumerate()
            .map(|(index, column)| {
                decode_inline_array(
                    &ducklake_to_arrow_type(&column.column_type)?,
                    &values_by_current_index[index],
                    StringEncoding::InlineStorage,
                )
            })
            .collect();

        batches.push(RecordBatch::try_new(current_schema.clone(), arrays?)?);
    }

    Ok(batches)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StringEncoding {
    InlineStorage,
    Literal,
}

fn decode_inline_array(
    data_type: &DataType,
    values: &[Option<String>],
    encoding: StringEncoding,
) -> Result<ArrayRef> {
    let mut builder = make_builder(data_type, values.len());
    for value in values {
        append_scalar_value(builder.as_mut(), data_type, value.as_deref(), encoding)?;
    }
    Ok(builder.finish())
}

fn append_scalar_value(
    builder: &mut dyn ArrayBuilder,
    data_type: &DataType,
    value: Option<&str>,
    encoding: StringEncoding,
) -> Result<()> {
    match data_type {
        DataType::Boolean => {
            let builder = downcast_builder::<BooleanBuilder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_bool(value)?),
                None => builder.append_null(),
            }
        },
        DataType::Int8 => {
            let builder = downcast_builder::<Int8Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_number::<i8>(value)?),
                None => builder.append_null(),
            }
        },
        DataType::Int16 => {
            let builder = downcast_builder::<Int16Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_number::<i16>(value)?),
                None => builder.append_null(),
            }
        },
        DataType::Int32 => {
            let builder = downcast_builder::<Int32Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_number::<i32>(value)?),
                None => builder.append_null(),
            }
        },
        DataType::Int64 => {
            let builder = downcast_builder::<Int64Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_number::<i64>(value)?),
                None => builder.append_null(),
            }
        },
        DataType::UInt8 => {
            let builder = downcast_builder::<UInt8Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_number::<u8>(value)?),
                None => builder.append_null(),
            }
        },
        DataType::UInt16 => {
            let builder = downcast_builder::<UInt16Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_number::<u16>(value)?),
                None => builder.append_null(),
            }
        },
        DataType::UInt32 => {
            let builder = downcast_builder::<UInt32Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_number::<u32>(value)?),
                None => builder.append_null(),
            }
        },
        DataType::UInt64 => {
            let builder = downcast_builder::<UInt64Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_number::<u64>(value)?),
                None => builder.append_null(),
            }
        },
        DataType::Float32 => {
            let builder = downcast_builder::<Float32Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_f32(value)?),
                None => builder.append_null(),
            }
        },
        DataType::Float64 => {
            let builder = downcast_builder::<Float64Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_f64(value)?),
                None => builder.append_null(),
            }
        },
        DataType::Utf8 => {
            let builder = downcast_builder::<StringBuilder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(decode_utf8(value, encoding)?),
                None => builder.append_null(),
            }
        },
        DataType::LargeUtf8 => {
            let builder = downcast_builder::<LargeStringBuilder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(decode_utf8(value, encoding)?),
                None => builder.append_null(),
            }
        },
        DataType::Binary => {
            let builder = downcast_builder::<BinaryBuilder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(decode_binary(value, encoding)?.as_slice()),
                None => builder.append_null(),
            }
        },
        DataType::LargeBinary => {
            let builder = downcast_builder::<LargeBinaryBuilder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(decode_binary(value, encoding)?.as_slice()),
                None => builder.append_null(),
            }
        },
        DataType::FixedSizeBinary(width) => {
            let builder = downcast_builder::<FixedSizeBinaryBuilder>(builder, data_type)?;
            match value {
                Some(value) => {
                    let bytes = decode_fixed_size_binary(value, *width, encoding)?;
                    builder.append_value(bytes.as_slice())?;
                },
                None => builder.append_null(),
            }
        },
        DataType::Date32 => {
            let builder = downcast_builder::<Date32Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_date32(value)?),
                None => builder.append_null(),
            }
        },
        DataType::Time64(TimeUnit::Microsecond) => {
            let builder = downcast_builder::<Time64MicrosecondBuilder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_time64_micros(value)?),
                None => builder.append_null(),
            }
        },
        DataType::Timestamp(TimeUnit::Second, tz) => {
            let builder = downcast_builder::<TimestampSecondBuilder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_timestamp(value, TimeUnit::Second, tz.is_some())?),
                None => builder.append_null(),
            }
        },
        DataType::Timestamp(TimeUnit::Millisecond, tz) => {
            let builder = downcast_builder::<TimestampMillisecondBuilder>(builder, data_type)?;
            match value {
                Some(value) => {
                    builder.append_value(parse_timestamp(value, TimeUnit::Millisecond, tz.is_some())?)
                },
                None => builder.append_null(),
            }
        },
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let builder = downcast_builder::<TimestampMicrosecondBuilder>(builder, data_type)?;
            match value {
                Some(value) => {
                    builder.append_value(parse_timestamp(value, TimeUnit::Microsecond, tz.is_some())?)
                },
                None => builder.append_null(),
            }
        },
        DataType::Timestamp(TimeUnit::Nanosecond, tz) => {
            let builder = downcast_builder::<TimestampNanosecondBuilder>(builder, data_type)?;
            match value {
                Some(value) => {
                    builder.append_value(parse_timestamp(value, TimeUnit::Nanosecond, tz.is_some())?)
                },
                None => builder.append_null(),
            }
        },
        DataType::Decimal128(precision, scale) => {
            let builder = downcast_builder::<Decimal128Builder>(builder, data_type)?;
            match value {
                Some(value) => builder.append_value(parse_decimal128(value, *precision, *scale)?),
                None => builder.append_null(),
            }
        },
        DataType::Decimal256(_, _) => {
            return Err(DuckLakeError::UnsupportedType(
                "Decimal256 inline decoding is not supported yet".to_string(),
            ));
        },
        DataType::List(field) => {
            let builder = downcast_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(builder, data_type)?;
            match value {
                Some(value) => {
                    for child in parse_list_literal(value)? {
                        append_scalar_value(
                            builder.values().as_mut(),
                            field.data_type(),
                            child.as_deref(),
                            StringEncoding::Literal,
                        )?;
                    }
                    builder.append(true);
                },
                None => builder.append(false),
            }
        },
        other => {
            return Err(DuckLakeError::UnsupportedType(format!(
                "Inline decoding for Arrow type '{}' is not supported yet",
                other
            )));
        },
    }

    Ok(())
}

fn downcast_builder<'a, T: 'static>(
    builder: &'a mut dyn ArrayBuilder,
    data_type: &DataType,
) -> Result<&'a mut T> {
    builder.as_any_mut().downcast_mut::<T>().ok_or_else(|| {
        DuckLakeError::Internal(format!(
            "Unexpected Arrow builder while decoding inline data for type '{}'",
            data_type
        ))
    })
}

fn parse_bool(value: &str) -> Result<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "t" | "1" => Ok(true),
        "false" | "f" | "0" => Ok(false),
        other => Err(DuckLakeError::InvalidConfig(format!(
            "Invalid inline boolean literal '{}'",
            other
        ))),
    }
}

fn parse_f32(value: &str) -> Result<f32> {
    match value.trim().to_ascii_lowercase().as_str() {
        "inf" | "infinity" => Ok(f32::INFINITY),
        "-inf" | "-infinity" => Ok(f32::NEG_INFINITY),
        "nan" => Ok(f32::NAN),
        _ => value.parse::<f32>().map_err(|e| {
            DuckLakeError::InvalidConfig(format!(
                "Invalid inline float32 literal '{}': {}",
                value, e
            ))
        }),
    }
}

fn parse_f64(value: &str) -> Result<f64> {
    match value.trim().to_ascii_lowercase().as_str() {
        "inf" | "infinity" => Ok(f64::INFINITY),
        "-inf" | "-infinity" => Ok(f64::NEG_INFINITY),
        "nan" => Ok(f64::NAN),
        _ => value.parse::<f64>().map_err(|e| {
            DuckLakeError::InvalidConfig(format!(
                "Invalid inline float64 literal '{}': {}",
                value, e
            ))
        }),
    }
}

fn decode_utf8(value: &str, encoding: StringEncoding) -> Result<String> {
    match encoding {
        StringEncoding::InlineStorage => {
            let bytes = decode_hex(value)?;
            String::from_utf8(bytes).map_err(|e| {
                DuckLakeError::InvalidConfig(format!(
                    "Invalid UTF-8 payload in inline BYTEA column: {}",
                    e
                ))
            })
        },
        StringEncoding::Literal => Ok(value.to_string()),
    }
}

fn decode_binary(value: &str, encoding: StringEncoding) -> Result<Vec<u8>> {
    match encoding {
        StringEncoding::InlineStorage => decode_hex(value),
        StringEncoding::Literal => Ok(value.as_bytes().to_vec()),
    }
}

fn decode_fixed_size_binary(
    value: &str,
    width: i32,
    encoding: StringEncoding,
) -> Result<Vec<u8>> {
    let bytes = match encoding {
        StringEncoding::InlineStorage => decode_hex(value)?,
        StringEncoding::Literal => decode_hex(value.trim_matches('\''))?,
    };

    if bytes.len() != width as usize {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Invalid fixed-size binary payload: expected {} bytes, got {}",
            width,
            bytes.len()
        )));
    }

    Ok(bytes)
}

fn decode_hex(value: &str) -> Result<Vec<u8>> {
    let value = value.trim();
    if value.len() % 2 != 0 {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Invalid hex literal length for inline payload '{}'",
            value
        )));
    }

    let mut bytes = Vec::with_capacity(value.len() / 2);
    let chars: Vec<char> = value.chars().collect();
    for index in (0..chars.len()).step_by(2) {
        let high = chars[index].to_digit(16).ok_or_else(|| {
            DuckLakeError::InvalidConfig(format!("Invalid hex digit '{}'", chars[index]))
        })?;
        let low = chars[index + 1].to_digit(16).ok_or_else(|| {
            DuckLakeError::InvalidConfig(format!("Invalid hex digit '{}'", chars[index + 1]))
        })?;
        bytes.push(((high << 4) | low) as u8);
    }
    Ok(bytes)
}

fn parse_date32(value: &str) -> Result<i32> {
    let date = NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d").map_err(|e| {
        DuckLakeError::InvalidConfig(format!(
            "Invalid inline date literal '{}': {}",
            value, e
        ))
    })?;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    Ok((date - epoch).num_days() as i32)
}

fn parse_time64_micros(value: &str) -> Result<i64> {
    let time = NaiveTime::parse_from_str(value.trim(), "%H:%M:%S%.f")
        .or_else(|_| NaiveTime::parse_from_str(value.trim(), "%H:%M:%S"))
        .map_err(|e| {
            DuckLakeError::InvalidConfig(format!(
                "Invalid inline time literal '{}': {}",
                value, e
            ))
        })?;
    Ok(i64::from(time.num_seconds_from_midnight()) * 1_000_000 + i64::from(time.nanosecond() / 1_000))
}

fn parse_timestamp(value: &str, unit: TimeUnit, with_timezone: bool) -> Result<i64> {
    let trimmed = value.trim();
    if with_timezone {
        let timestamp = parse_timestamp_tz(trimmed)?;
        return Ok(match unit {
            TimeUnit::Second => timestamp.timestamp(),
            TimeUnit::Millisecond => timestamp.timestamp_millis(),
            TimeUnit::Microsecond => timestamp.timestamp_micros(),
            TimeUnit::Nanosecond => timestamp
                .timestamp_nanos_opt()
                .ok_or_else(|| {
                    DuckLakeError::InvalidConfig(format!(
                        "Timestamp '{}' is out of range for nanosecond precision",
                        trimmed
                    ))
                })?,
        });
    }

    let naive = parse_naive_timestamp(trimmed)?;
    let timestamp = naive.and_utc();
    Ok(match unit {
        TimeUnit::Second => timestamp.timestamp(),
        TimeUnit::Millisecond => timestamp.timestamp_millis(),
        TimeUnit::Microsecond => timestamp.timestamp_micros(),
        TimeUnit::Nanosecond => timestamp
            .timestamp_nanos_opt()
            .ok_or_else(|| {
                DuckLakeError::InvalidConfig(format!(
                    "Timestamp '{}' is out of range for nanosecond precision",
                    trimmed
                ))
            })?,
    })
}

fn parse_naive_timestamp(value: &str) -> Result<NaiveDateTime> {
    for format in [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
    ] {
        if let Ok(timestamp) = NaiveDateTime::parse_from_str(value, format) {
            return Ok(timestamp);
        }
    }

    Err(DuckLakeError::InvalidConfig(format!(
        "Invalid inline timestamp literal '{}'",
        value
    )))
}

fn parse_timestamp_tz(value: &str) -> Result<DateTime<Utc>> {
    if let Ok(timestamp) = DateTime::parse_from_rfc3339(value) {
        return Ok(timestamp.with_timezone(&Utc));
    }

    for format in [
        "%Y-%m-%d %H:%M:%S%.f%:z",
        "%Y-%m-%d %H:%M:%S%:z",
        "%Y-%m-%dT%H:%M:%S%.f%:z",
        "%Y-%m-%dT%H:%M:%S%:z",
    ] {
        if let Ok(timestamp) = DateTime::<FixedOffset>::parse_from_str(value, format) {
            return Ok(timestamp.with_timezone(&Utc));
        }
    }

    Err(DuckLakeError::InvalidConfig(format!(
        "Invalid inline timestamptz literal '{}'",
        value
    )))
}

fn parse_decimal128(value: &str, precision: u8, scale: i8) -> Result<i128> {
    let trimmed = value.trim();
    let negative = trimmed.starts_with('-');
    let unsigned = trimmed.strip_prefix('-').unwrap_or(trimmed);

    let (whole, fractional) = match unsigned.split_once('.') {
        Some((whole, fractional)) => (whole, fractional),
        None => (unsigned, ""),
    };

    if fractional.len() > scale as usize {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Decimal literal '{}' exceeds scale {}",
            value, scale
        )));
    }

    let mut digits = String::with_capacity(whole.len() + scale as usize);
    digits.push_str(whole);
    digits.push_str(fractional);
    while digits.len() < whole.len() + scale as usize {
        digits.push('0');
    }

    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    if digits.len() > precision as usize {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Decimal literal '{}' exceeds precision {}",
            value, precision
        )));
    }

    let mut parsed = parse_number::<i128>(digits)?;
    if negative {
        parsed = -parsed;
    }
    Ok(parsed)
}

fn parse_list_literal(value: &str) -> Result<Vec<Option<String>>> {
    let trimmed = value.trim();
    if trimmed == "[]" {
        return Ok(Vec::new());
    }
    if !trimmed.starts_with('[') || !trimmed.ends_with(']') {
        return Err(DuckLakeError::InvalidConfig(format!(
            "Invalid inline list literal '{}'",
            value
        )));
    }

    let mut values = Vec::new();
    let mut current = String::new();
    let mut quote: Option<char> = None;
    let mut chars = trimmed[1..trimmed.len() - 1].chars().peekable();

    while let Some(ch) = chars.next() {
        if let Some(active_quote) = quote {
            if ch == active_quote {
                if chars.peek() == Some(&active_quote) {
                    current.push(active_quote);
                    chars.next();
                } else {
                    quote = None;
                }
            } else if ch == '\\' {
                if let Some(escaped) = chars.next() {
                    current.push(escaped);
                }
            } else {
                current.push(ch);
            }
            continue;
        }

        match ch {
            '\'' | '"' => quote = Some(ch),
            ',' => {
                values.push(parse_list_token(&current)?);
                current.clear();
            },
            _ => current.push(ch),
        }
    }

    values.push(parse_list_token(&current)?);
    Ok(values)
}

fn parse_list_token(token: &str) -> Result<Option<String>> {
    let trimmed = token.trim();
    if trimmed.is_empty() {
        return Ok(Some(String::new()));
    }
    if trimmed.eq_ignore_ascii_case("null") {
        return Ok(None);
    }
    Ok(Some(trimmed.to_string()))
}

fn parse_number<T>(value: &str) -> Result<T>
where
    T: std::str::FromStr,
    T::Err: std::fmt::Display,
{
    value.trim().parse::<T>().map_err(|e| {
        DuckLakeError::InvalidConfig(format!(
            "Invalid inline numeric literal '{}': {}",
            value, e
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata_provider::DuckLakeTableColumn;
    use async_trait::async_trait;
    use arrow::array::Array;

    #[test]
    fn parses_list_literals_with_quotes_and_nulls() {
        let parsed = parse_list_literal("[1, NULL, 'hello, world', \"quoted\"]").unwrap();
        assert_eq!(
            parsed,
            vec![
                Some("1".to_string()),
                None,
                Some("hello, world".to_string()),
                Some("quoted".to_string())
            ]
        );
    }

    #[test]
    fn decodes_inline_utf8_from_hex() {
        assert_eq!(
            decode_utf8("68656c6c6f", StringEncoding::InlineStorage).unwrap(),
            "hello"
        );
    }

    #[test]
    fn parses_decimal128_literals() {
        assert_eq!(parse_decimal128("12.34", 10, 2).unwrap(), 1234);
        assert_eq!(parse_decimal128("-0.50", 10, 2).unwrap(), -50);
    }

    #[derive(Debug)]
    struct MockInliningReader;

    #[async_trait]
    impl CatalogInliningReader for MockInliningReader {
        async fn get_inlined_table_refs(&self, _table_id: i64) -> Result<Vec<InlinedTableRef>> {
            Ok(vec![InlinedTableRef {
                table_name: "ducklake_inlined_data_1_1".to_string(),
                schema_version: 1,
            }])
        }

        async fn get_historical_schema(
            &self,
            _table_id: i64,
            _schema_version: i64,
        ) -> Result<Vec<DuckLakeTableColumn>> {
            Ok(vec![
                DuckLakeTableColumn::new(1, "id".to_string(), "int64".to_string(), false),
                DuckLakeTableColumn::new(2, "name".to_string(), "varchar".to_string(), true),
            ])
        }

        async fn read_visible_inlined_rows(
            &self,
            _table_name: &str,
            _columns: &[DuckLakeTableColumn],
            _snapshot_id: i64,
        ) -> Result<Vec<InlineRow>> {
            Ok(vec![
                InlineRow {
                    row_id: 1,
                    values: vec![Some("1".to_string()), Some("416c696365".to_string())],
                },
                InlineRow {
                    row_id: 2,
                    values: vec![Some("2".to_string()), Some("426f62".to_string())],
                },
            ])
        }

        async fn get_inlined_file_deletes(
            &self,
            _table_id: i64,
            _snapshot_id: i64,
        ) -> Result<Vec<InlinedFileDelete>> {
            Ok(Vec::new())
        }
    }

    #[tokio::test]
    async fn remaps_historical_inline_rows_to_current_schema() {
        let current_columns = vec![
            DuckLakeTableColumn::new(1, "id".to_string(), "int64".to_string(), false),
            DuckLakeTableColumn::new(2, "full_name".to_string(), "varchar".to_string(), true),
            DuckLakeTableColumn::new(3, "age".to_string(), "int32".to_string(), true),
        ];

        let batches = load_visible_inlined_batches(&MockInliningReader, 1, 10, &current_columns)
            .await
            .unwrap();

        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.schema().field(1).name(), "full_name");

        let ids = batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let names = batch
            .column(1)
            .as_any()
            .downcast_ref::<arrow::array::StringArray>()
            .unwrap();
        let ages = batch
            .column(2)
            .as_any()
            .downcast_ref::<arrow::array::Int32Array>()
            .unwrap();

        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);
        assert_eq!(names.value(0), "Alice");
        assert_eq!(names.value(1), "Bob");
        assert!(ages.is_null(0));
        assert!(ages.is_null(1));
    }
}

//! Serializer: [`QueryResult`] → bytes in a [`QueryFormat`].
//!
//! Stateless; every function is a pure transformation. This is the boundary
//! between LENS's Arrow world and HTTP: nothing past it sees an Arrow type.
//!
//! # JSON
//!
//! ```json
//! {"rows":[{"slot":295000000,"blockhash":"4vJ9Sh..."}],"row_count":1,"duration_ms":47,"truncated":false,"sql":"SELECT ..."}
//! ```
//!
//! The rows are written by Arrow's JSON [`ArrayWriter`](json::ArrayWriter)
//! and spliced in as-is; the wrapper fields are encoded with `serde_json`.
//! The row bytes are never parsed back, so no value passes through an `f64`:
//!
//! * `UInt64` / `Int64` columns (`slot`, `fee`, `amount_raw`, ...) are
//!   written as exact JSON integers, up to `u64::MAX`. Note that JavaScript's
//!   `JSON.parse` rounds integers above 2^53; clients that need exact token
//!   amounts must parse with a big-integer aware decoder, or use CSV.
//! * Nulls are written as explicit `null`s, so every row object has every
//!   column.
//! * Timestamps (`block_time`) are RFC 3339 strings with their offset.
//!
//! # CSV
//!
//! A header line of column names, then one line per row, quoted per
//! RFC 4180. A null is an empty field; timestamps are RFC 3339.
//!
//! # Empty results
//!
//! With no batches, JSON has `"rows":[]` and CSV is the header line alone,
//! taken from [`QueryResult::schema`].

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::{csv, json};

use super::{QueryFormat, QueryResult, SerializedResult};
use crate::error::{LensError, LensResult};

/// Serialize `result` to `format`.
pub fn serialize(result: QueryResult, format: QueryFormat) -> LensResult<SerializedResult> {
    let bytes = match format {
        QueryFormat::Json => serialize_json(&result)?,
        QueryFormat::Csv => serialize_csv(&result)?,
    };
    Ok(SerializedResult {
        bytes,
        format,
        row_count: result.row_count,
        duration_ms: result.duration_ms,
        truncated: result.truncated,
        sql: result.sql,
    })
}

/// `result` as a JSON object: `rows`, `row_count`, `duration_ms`,
/// `truncated`, `sql`, in that order.
pub fn serialize_json(result: &QueryResult) -> LensResult<Vec<u8>> {
    let err = |e: crate::error::BoxError| LensError::serialization(QueryFormat::Json.as_str(), e);
    let mut out = Vec::with_capacity(64 + result.sql.len());

    out.extend_from_slice(b"{\"rows\":");
    let batches: Vec<&RecordBatch> = result.batches.iter().filter(|b| b.num_rows() > 0).collect();
    if batches.is_empty() {
        out.extend_from_slice(b"[]");
    } else {
        let mut writer = json::WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, json::writer::JsonArray>(&mut out);
        writer.write_batches(&batches).map_err(|e| err(e.into()))?;
        writer.finish().map_err(|e| err(e.into()))?;
    }

    for (key, value) in [
        ("row_count", serde_json::Value::from(result.row_count)),
        ("duration_ms", serde_json::Value::from(result.duration_ms)),
        ("truncated", serde_json::Value::from(result.truncated)),
        ("sql", serde_json::Value::from(result.sql.as_str())),
    ] {
        out.push(b',');
        serde_json::to_writer(&mut out, key).map_err(|e| err(e.into()))?;
        out.push(b':');
        serde_json::to_writer(&mut out, &value).map_err(|e| err(e.into()))?;
    }
    out.push(b'}');
    Ok(out)
}

/// `result` as CSV with a header line.
pub fn serialize_csv(result: &QueryResult) -> LensResult<Vec<u8>> {
    let err = |e: datafusion::arrow::error::ArrowError| {
        LensError::serialization(QueryFormat::Csv.as_str(), e)
    };
    let mut out = Vec::new();
    {
        let mut writer = csv::WriterBuilder::new().with_header(true).build(&mut out);
        if result.batches.is_empty() {
            // The writer only emits the header along with a batch.
            writer.write(&RecordBatch::new_empty(result.schema.clone())).map_err(err)?;
        }
        for batch in &result.batches {
            writer.write(batch).map_err(err)?;
        }
    }
    Ok(out)
}

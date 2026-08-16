---
myst:
  html_meta:
    description: "Review the explicit Apache Doris scalar-to-Arrow schema mapping, projection order, nullability checks, decimals, timestamps, and unsupported types."
---

(ray-doris-work-with-schemas)=

# Work with schemas

`ray-doris` discovers schema with MySQL `DESCRIBE` and builds one canonical PyArrow schema before creating worker tasks. It doesn't infer types from returned values.

The same mapping is used by writes. A write validates a full-row Arrow schema against the existing
Doris column order, or validates a known subset for Merge-on-Write partial updates, before sending a
Stream Load request.

## Review supported scalar types

The connector maps these Doris types:

| Doris type | PyArrow type |
| --- | --- |
| `BOOLEAN` | `bool` |
| `TINYINT` | `int8` |
| `SMALLINT` | `int16` |
| `INT`, `INTEGER` | `int32` |
| `BIGINT` | `int64` |
| `LARGEINT` | `decimal256(39, 0)` |
| `FLOAT` | `float32` |
| `DOUBLE` | `float64` |
| `CHAR`, `VARCHAR`, `STRING` | `string` |
| `JSON`, `JSONB` | `string` |
| `DATE`, `DATEV2` | `date32` |
| `DATETIME`, `DATETIMEV2` | `timestamp[us]` |
| Doris decimal families with precision up to 38 | `decimal128(precision, scale)` |
| Doris decimal families with precision from 39 through 76 | `decimal256(precision, scale)` |

Supported decimal families include `DECIMAL`, `DECIMALV2`, `DECIMALV3`, `DECIMAL32`, `DECIMAL64`, `DECIMAL128`, and `DECIMAL256`. A decimal declaration must include valid precision and scale. `TIME` and `TIMEV2` are intentionally unsupported in the RFC pre-review type matrix.

## Preserve projection and nullability

When you pass `columns`, the Arrow schema follows that order rather than the physical table order. Each field preserves the `YES` or `NO` nullability returned by `DESCRIBE`.

MySQL rows convert into arrays with the planned type. Flight columns use safe Arrow casts. Both readers verify that result-column names match the schema and reject a null value in a non-nullable field with the same public error classification.

## Handle decimal and temporal values

The MySQL reader converts decimal-compatible values through their string form and then constructs `decimal.Decimal`. It doesn't round-trip a decimal through binary floating point.

`DATETIMEV2` uses microsecond-resolution `timestamp[us]`. The required integration suite compares MySQL and Flight values at microsecond precision.

Doris zero dates such as `0000-00-00` don't have a lossless Arrow date or timestamp representation. Conversion fails and identifies the affected column instead of coercing the value to null.

## Reject unsupported types

The connector fails closed for nested prefixes `ARRAY<...>`, `MAP<...>`, and `STRUCT<...>`. It also rejects `AGG_STATE`, `BITMAP`, `HLL`, `QUANTILE_STATE`, `VARIANT`, malformed declarations, and any unknown type.

Project only the supported scalar columns when a table contains an unsupported field:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    columns=["event_id", "created_at", "score"],
)
```

An unsupported projected column raises {ref}`DorisSchemaError <ray-doris-api-schema-error>` during planning. The connector doesn't silently stringify it.

Writes additionally reject timezone-aware temporal values, non-finite floating-point or decimal
values, unsafe numeric casts, and NULL values for non-nullable columns before uploading. JSON and
JSONB are represented as strings for full-row Parquet writes; partial-update JSON values may use
JSON-compatible scalar, list, and object values.

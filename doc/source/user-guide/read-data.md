---
myst:
  html_meta:
    description: "Read one Apache Doris internal-catalog table with ray-doris, including projection, trusted filters, empty results, and planning fallback."
---

(ray-doris-read-data)=

# Read data

Use {ref}`read_doris <ray-doris-api-read-doris>` for normal application code. It validates connector options, creates a {ref}`DorisDatasource <ray-doris-api-datasource>`, and passes the datasource to Ray's public `ray.data.read_datasource()` API.

## Select a table

Pass one exact `database.table` name:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    user="ray_reader",
    password="...",
)
```

Database, table, and column identifiers must start with a letter or underscore and then contain only letters, numbers, underscores, or dollar signs. `ray-doris` quotes validated identifiers in generated SQL.

Three-part names such as `internal.analytics.events` fail before network access. External catalogs, joins, subqueries in the table argument, and arbitrary `SELECT` statements aren't supported.

## Project columns

Use `columns` to preserve only the required fields and their requested order:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    user="ray_reader",
    password="...",
    columns=["event_id", "score", "created_at"],
)
```

The list must be non-empty and can't contain duplicates. Schema discovery rejects a projected name that `DESCRIBE` doesn't return.

## Apply a trusted filter

Use `filter` for a trusted Doris SQL scalar expression:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    user="ray_reader",
    password="...",
    filter="score >= 80 AND created_at >= '2026-01-01'",
)
```

`ray-doris` rejects statement separators, SQL comment tokens outside quoted literals, and unterminated quotes. It doesn't parse or parameterize the expression. Never build `filter` from untrusted request values.

Doris uses the filter during `_query_plan`, so predicate pruning can reduce the returned tablet set. The worker uses the same expression in each split query.

## Choose planning failure behavior

The default `on_query_plan_error="single_task"` keeps a trusted query executable when Doris can run the SQL but can't represent it as tablet splits. The fallback removes the `TABLET` hint and creates one task:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    user="ray_reader",
    password="...",
    filter="event_id IN (SELECT event_id FROM analytics.selected_events)",
    on_query_plan_error="single_task",
)
```

Use `on_query_plan_error="error"` when planning failures must stop the read:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    user="ray_reader",
    password="...",
    on_query_plan_error="error",
)
```

Authentication and permission errors always stop planning, regardless of this setting.

## Handle empty results

A filter that prunes every tablet is a successful empty read. Ray receives a schema-carrying zero-row Arrow block, so both operations remain valid:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    filter="1 = 0",
)

assert dataset.take_all() == []
assert dataset.schema() is not None
```

An existing table with no rows also returns an empty Dataset. Neither case falls back to a full-table scan.

## Use the datasource directly

Use {ref}`DorisDatasource <ray-doris-api-datasource>` when you need Ray options that the convenience function doesn't expose. Pass only keyword arguments supported by your installed Ray version:

```python
import ray.data

from ray_doris import DorisDatasource

datasource = DorisDatasource(
    table="analytics.events",
    host="doris-fe.example.com",
    tablet_size=16,
)
dataset = ray.data.read_datasource(
    datasource,
    concurrency=4,
    override_num_blocks=32,
)
```

Treat one datasource instance as one logical read because it caches its initial schema and tablet discovery result.

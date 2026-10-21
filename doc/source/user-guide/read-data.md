---
myst:
  html_meta:
    description: "Read one Apache Doris internal-catalog table with ray-doris, including projection, trusted filters, empty results, and planning fallback."
---

(ray-doris-read-data)=

# Read data

Use {ref}`read_doris <ray-doris-api-read-doris>` for normal application code. By default, it validates connector options, creates a {ref}`DorisDatasource <ray-doris-api-datasource>`, and passes the datasource to Ray's public `ray.data.read_datasource()` API.

## Select a table

Pass one exact `database.table` name:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    user="ray_reader",
    password_env="DORIS_PASSWORD",
)
```

`password_env` is the recommended credential boundary for distributed use. See
[Secure connections](secure-connections.md) for driver/worker injection and TLS requirements.

Database, table, and column identifiers must start with a letter or underscore and then contain only letters, numbers, underscores, or dollar signs. `ray-doris` quotes validated identifiers in generated SQL.

Three-part names such as `internal.analytics.events` fail before network access.
For experimental external-table reads, supply `catalog` separately and retain the
two-part `table` name. The optional `auto_native_read=True` switch selects native
Hive Parquet reading only when its profile applies; see
[shared-storage reads](native-read.md) for the source-selection and authorization
boundaries. Joins, subqueries in the table argument and arbitrary `SELECT`
statements aren't supported.

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

The list must be non-empty and can't contain duplicates. Schema discovery rejects a projected name that Doris metadata doesn't return.

On verified Ray 2.58.0 and 2.59.0 releases, Dataset column selections can also
prune the Doris scan automatically:

```python
dataset = read_doris(table="analytics.events", host="doris-fe.example.com")
selected = dataset.select_columns(["score", "event_id"])
```

When Ray pushes this selection into the V1 datasource, both planning SQL and
worker SQL select the requested columns in that order. Types and nullability
are preserved. An explicit `columns` list limits the available columns; a later
selection cannot restore a column omitted earlier. Renames and computed
expressions remain Ray operations.

Each projection produces a new datasource. A changed selection or order clears
the clone's planning cache, leaving other Dataset branches unchanged. Repeated
planning of the same clone still reuses its discovery snapshot. This is not
database snapshot isolation.

Ray operations such as residual filters can prevent a selection from reaching
the read. Keep their dependency columns available. Accepted native predicates
are retained when a later projection removes their columns from the output.
See [native Dataset predicates](#use-native-dataset-predicates) for the supported
subset. A trusted SQL `filter` can also reference a column that is not selected
for output.

An optimizer-requested empty projection preserves Ray's native row-count
semantics using its documented block interface. It retains the previous
physical scan columns and applies the empty projection to bounded output
batches, so this case does not reduce Doris column transfer. Public
`read_doris(columns=[])` remains invalid. An empty materialized Datasource read
may have unknown schema, as it does without this optimization.

Automatic column pruning is disabled on other accepted Ray versions, including
unverified patch releases. Explicit `columns` works throughout the supported
window. The version gate also checks the public hook signatures before enabling
the optimization.

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

For internal tables, Doris uses the filter during `_query_plan`, so predicate pruning can reduce the returned tablet set. The worker uses the same expression in each split query. An external-table filter retains Doris execution through one fully qualified query without tablet planning.

## Use native Dataset predicates

On Ray 2.57, 2.58, and 2.59, use expressions from `ray.data.expressions` to filter a Dataset:

```python
from ray.data.expressions import col

filtered = dataset.filter(expr=(col("score") >= 80) & col("score").is_not_null())
```

The datasource accepts comparisons between a signed 8-, 16-, 32-, or 64-bit integer column and
an in-range Python integer literal, boolean equality and inequality with Python boolean literals,
and null checks on these column types. It accepts `AND`, `OR`, and `NOT` only when the complete
predicate supplied by Ray is supported. Comparing with `None` is not rewritten into a null check.
IN, floating-point, decimal, string, date/time, arithmetic, column-to-column comparisons, and user
functions remain in Ray. Use the explicit trusted SQL `filter` for other Doris expressions.

Ray may split its own convertible conjuncts from a residual filter. The connector never drops an
unsupported child: it returns the original datasource to preserve Ray's Filter. Column names and
types are checked against the visible output schema before acceptance; missing columns keep Ray's
normal error behavior. The expression nodes are DeveloperAPI alpha, so native predicate support is limited
to the verified Ray minors. Earlier supported Ray versions retain their existing reads.

Accepted predicates are combined with the existing trusted SQL filter using parenthesized AND.
MySQL scan values use parameter binding. HTTP query planning and Flight use validated integer and
boolean literals. New predicates create independent datasource clones and invalidate tablet
planning; they do not change other Dataset branches or provide a database snapshot. A clone may
repeat schema discovery. SQL LIMIT is not generated: Ray retains residual filtering and limit
ordering. Native predicates do not promise output ordering or a particular reduction in tablet count.

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

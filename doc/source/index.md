---
myst:
  html_meta:
    description: "ray-doris reads Apache Doris internal-catalog tables and writes bounded Stream Load batches through Ray Data public Datasource and Datasink APIs."
---

(ray-doris-main)=

# ray-doris: Apache Doris reads and writes for Ray Data

`ray-doris` is an independent, community-maintained Apache Doris connector for Ray Data. It plans reads with the Doris frontend query-plan endpoint, assigns tablet groups to Ray read tasks, streams rows through MySQL or Arrow Flight SQL, and writes bounded Parquet or line-delimited JSON batches through HTTP Stream Load without importing Ray Data internals.

The project is alpha software and isn't maintained or endorsed by the Ray or Apache Doris projects. See [Compatibility](compatibility.md) for the tested version matrix and [Key concepts](key-concepts.md) for the execution model.

MySQL is the production-candidate transport. Flight SQL and automatic transport selection remain experimental.

Writes use `write_doris()` or `DorisDatasink` with explicit operation and table-model validation.
The RFC pre-review profile disables Ray task retries, treats unknown Stream Load outcomes as ambiguous, and
does not claim exactly-once or whole-dataset atomicity. See [Write data](user-guide/write-data.md).

## Read a table

The public entry point returns a {py:class}`ray.data.Dataset`:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    user="ray_reader",
    password_env="DORIS_PASSWORD",
    columns=["event_id", "created_at", "score"],
    filter="score >= 80",
    tablet_size=32,
    override_num_blocks=64,
)

print(dataset.schema())
print(dataset.take(5))
```

`table` must name one internal-catalog table in exact `database.table` form. The `filter` value is a trusted SQL scalar expression, not a parameterized user-input boundary.

## Learn more

::::{grid} 1 2 2 3
:gutter: 1

:::{grid-item-card} Quickstart
:link: quickstart
:link-type: doc

Install from a source checkout and run your first Doris read.
:::

:::{grid-item-card} Key concepts
:link: key-concepts
:link-type: doc

Understand driver planning, tablet splits, worker streams, and Ray blocks.
:::

:::{grid-item-card} User guides
:link: user-guide/index
:link-type: doc

Configure projections, transports, parallelism, schemas, and secure connections.
:::

:::{grid-item-card} Write data
:link: user-guide/write-data
:link-type: doc

Use Stream Load safely for load, upsert, and Merge-on-Write partial updates.
:::

:::{grid-item-card} Examples
:link: examples/index
:link-type: doc

Adapt source-aligned examples for MySQL, Flight SQL, and advanced Ray options.
:::

:::{grid-item-card} API reference
:link: api/api
:link-type: doc

Read generated signatures and docstrings for every public symbol.
:::

:::{grid-item-card} Architecture
:link: architecture
:link-type: doc

Trace planning, task construction, streaming conversion, and failure boundaries.
:::
::::

```{toctree}
:hidden:
:maxdepth: 4

Quickstart <quickstart>
Key concepts <key-concepts>
User guides <user-guide/index>
Examples <examples/index>
FAQ <faq>
API reference <api/api>
Compatibility <compatibility>
Architecture <architecture>
Contributing <contributing/index>
```

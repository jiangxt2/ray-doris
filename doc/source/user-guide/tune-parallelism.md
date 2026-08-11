---
myst:
  html_meta:
    description: "Tune ray-doris tablet grouping, Ray read-task concurrency, output block planning, and transport batch size without confusing their roles."
---

(ray-doris-tune-parallelism)=

# Tune parallelism

Start with Doris tablet distribution, then set Ray scheduling controls. A larger output block request can't create more independent Doris scans than the tablet plan contains.

## Group tablets into read tasks

`tablet_size` is a soft grouping target. For a non-empty plan, the datasource computes this task count:

```text
min(Ray_parallelism_hint, ceil(number_of_tablets / tablet_size))
```

It then assigns sorted tablet IDs to that many deterministic adjacent groups. Group sizes differ by at most one. `tablet_size=1` exposes the finest tablet-level split allowed by Ray's hint, while a larger value reduces Ray scheduling overhead for tables with many tablets.

The Ray parallelism hint is also a cap. If Doris returns 48 tablets, `tablet_size=1`, and Ray requests parallelism 12, the datasource creates at most 12 groups rather than 48 tasks.

## Limit active reads

Use `concurrency` to limit how many read tasks run at the same time:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    tablet_size=4,
    concurrency=8,
)
```

This setting controls Ray execution pressure. It doesn't change the number of tablets in Doris or the number of rows in a worker batch.

## Request output blocks

Use `override_num_blocks` to influence Ray's output block planning:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    tablet_size=8,
    concurrency=4,
    override_num_blocks=32,
)
```

The source split count and final Dataset block count can differ because Ray can combine or split datasource output according to its execution plan. A single-task query-plan fallback still has one source query even when `override_num_blocks` requests more output blocks.

## Bound worker output

`batch_size` is a hard `fetchmany()` row bound for MySQL and a hard row bound for Arrow tables emitted by the Flight reader:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    batch_size=5_000,
)
```

For Flight SQL, this value slices a RecordBatch after the Arrow Database Connectivity (ADBC) driver receives it. It doesn't cap the server RecordBatch or ADBC prefetch allocation.

## Assign Ray task resources

Use `ray_remote_args` for supported Ray remote options:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    concurrency=3,
    ray_remote_args={
        "num_cpus": 1,
        "scheduling_strategy": "SPREAD",
        "max_retries": 3,
    },
)
```

Ray version support determines which options are valid. `ray-doris` passes the mapping to `ray.data.read_datasource()` without maintaining a separate option schema.

## Select settings from evidence

Measure task duration, worker memory, Doris backend load, and Ray scheduling overhead with your table distribution. The repository's full slow suite verifies three Ray workers, three Doris backends, 48 tablets, retries, and backend failover. The `core` profile verifies only tablet task count and three-worker distribution. These are functional distributed gates, not performance benchmarks, so they don't define production defaults.

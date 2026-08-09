---
myst:
  html_meta:
    description: "Understand how ray-doris discovers schemas, prunes Doris tablets, creates Ray read tasks, and emits bounded Arrow blocks."
---

(ray-doris-key-concepts)=

# Key concepts

`ray-doris` separates driver-side metadata planning from worker-side data movement. This boundary keeps Doris connections out of Ray's internal APIs and makes each read task serializable.

## How does a read work?

One datasource instance follows this sequence:

```text
read_doris()
  -> DorisDatasource
  -> MySQL DESCRIBE
  -> Doris FE _query_plan
  -> tablet groups
  -> Ray ReadTask objects
  -> streaming MySQL or Flight SQL reads
  -> canonical PyArrow tables
  -> Ray Dataset
```

The driver validates configuration without network access, discovers the projected schema with `DESCRIBE`, and sends the base `SELECT` to the Doris frontend `_query_plan` endpoint. Doris applies partition and tablet pruning to the trusted filter expression. `ray-doris` groups the returned tablet IDs and creates one Ray `ReadTask` for each group.

Each worker opens its own transport connection and executes a `SELECT` with a Doris `TABLET(...)` hint. The worker yields PyArrow tables instead of collecting the split into one in-memory result.

## Driver and worker responsibilities

The two execution locations have distinct responsibilities:

| Location | Responsibility |
| --- | --- |
| Driver | Validate identifiers and options, discover schema, request the tablet plan, group tablets, and construct serializable tasks |
| Worker | Open the selected transport, execute one split query, convert values to the planned Arrow schema, and close protocol resources |

The driver doesn't run a count or sample query for Ray auto-parallelism. `estimate_inmemory_data_size()` returns `None` because an extra data query would add cost and could observe a different table state.

## Tablets, tasks, and blocks

A *tablet* is a Doris storage shard selected by the query plan. A *read task* is a Ray scheduling unit that owns one or more tablets. A *block* is a PyArrow table yielded by a read task.

These settings control different layers:

| Setting | Layer | Effect |
| --- | --- | --- |
| `tablet_size` | Doris split planning | Sets the minimum target number of adjacent tablets in one read task |
| Ray `parallelism` hint | Datasource planning | Caps the number of tablet groups returned to Ray |
| `concurrency` | Ray execution | Limits the number of read tasks that execute at the same time |
| `batch_size` | Worker output | Bounds MySQL `fetchmany()` batches and slices Flight results into Ray blocks |
| `override_num_blocks` | Ray output planning | Requests an output block count independently of the source tablet count |

See [Tune parallelism](user-guide/tune-parallelism.md) before changing more than one setting.

## Canonical schema

Schema discovery always uses the frontend MySQL port, even when workers read through Flight SQL. This produces one explicit Arrow schema for every split and transport. Workers reject result-column drift, unsafe casts, and nulls in fields planned as non-nullable.

Supported Doris scalar types map without inference. Unsupported nested, aggregate-state, and unknown types raise {ref}`DorisSchemaError <ray-doris-api-schema-error>` during planning. See [Work with schemas](user-guide/work-with-schemas.md) for the type matrix.

## Planning fallback

Doris can return HTTP 200 while reporting a planning error inside the JSON body. `ray-doris` validates the outer `code`, inner `status`, `exception`, and `partitions` fields before accepting a plan.

With `on_query_plan_error="single_task"`, a non-access planning failure produces one query without a `TABLET` hint. Authentication and permission failures never use this fallback. TLS validation failures and authenticated HTTP redirects also fail closed.

An empty `partitions` object from a successful query plan represents a valid empty result. The datasource returns a zero-row Arrow block that preserves the planned schema.

## Consistency and retries

Tablet planning and split execution don't provide snapshot isolation. Concurrent writes can make different tasks observe different moments. If Ray retries a failed task after it emitted part of a split, the replacement task reads the complete split again. `ray-doris` doesn't resume from a partial row offset.

Create a new {ref}`DorisDatasource <ray-doris-api-datasource>` for each logical read. A datasource instance caches its first schema and tablet discovery result because Ray can request read tasks more than once while constructing a Dataset. The cache avoids repeated planning calls but doesn't create a database snapshot.

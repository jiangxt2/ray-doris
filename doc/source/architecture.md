---
myst:
  html_meta:
    description: "ray-doris architecture, including public API adaptation, driver planning, schema mapping, Ray task construction, worker transports, and failure boundaries."
---

(ray-doris-architecture)=

# Architecture

`ray-doris` is a thin independent connector around public Ray Data V1 interfaces and Doris public protocols. Ray owns distributed scheduling and Dataset execution. Doris owns query semantics, tablet pruning, and storage replicas. The connector owns validation, protocol coordination, schema consistency, and failure classification.

## Follow the module boundaries

The package separates public entry points from protocol implementation:

```text
src/ray_doris/
├── __init__.py    public exports and package version
├── _api.py        read_doris() convenience entry point
├── datasource.py  Ray Datasource and ReadTask construction
├── _models.py     immutable configuration, plans, and splits
├── _planner.py    MySQL schema discovery and HTTP tablet planning
├── _readers.py    streaming MySQL and Flight SQL worker readers
├── _schema.py     Doris-to-Arrow schema mapping and conversion
├── _sql.py        identifier validation and SQL rendering
├── _compat.py     explicit supported-Ray constructor adaptation
└── _errors.py     public exception hierarchy
```

Only `read_doris`, `DorisDatasource`, and the exception hierarchy are public package exports. The underscore-prefixed modules can change without a public compatibility promise.

## Adapt the public Ray API

{ref}`read_doris <ray-doris-api-read-doris>` builds a datasource and calls `ray.data.read_datasource()` with `concurrency`, `override_num_blocks`, and a copied `ray_remote_args` mapping.

{ref}`DorisDatasource <ray-doris-api-datasource>` subclasses Ray's public `Datasource`. It returns `ReadTask` objects that yield PyArrow tables. `_compat.py` inspects the public `ReadTask` constructor so it can reject unsupported Ray signatures without catching an unrelated `TypeError` from the read function.

## Plan on the driver

The driver performs two metadata operations. MySQL `DESCRIBE` produces column names, types, and nullability. An authenticated HTTP POST to `/_query_plan` produces predicate-pruned tablet IDs.

The query-plan client disables redirects and validates both HTTP failures and Doris's body envelope. Doris can report application errors inside an HTTP 200 response, so HTTP status alone isn't enough.

Schema and tablet IDs form one immutable planning snapshot. `get_read_tasks()` reuses it when Ray requests tasks more than once, then groups the same tablet IDs using the new parallelism hint.

## Execute on workers

Each task closes its cursor and connection in nested `finally` blocks. The MySQL reader uses a server-side cursor and converts each fetched row batch to the canonical schema. The Flight reader consumes RecordBatch objects, uses safe Arrow casts, verifies nullability, and slices tables into connector output batches.

Transport option mappings are deep-copied into immutable tuple storage so caller mutation can't change a constructed datasource. The configuration representation exposes option keys for diagnosis but redacts the password and every option value.

## Keep SQL generation narrow

The connector generates only two statement shapes:

```text
DESCRIBE `database`.`table`
SELECT <projection> FROM `database`.`table` [TABLET(...)] [WHERE <trusted expression>]
```

The connector restricts and quotes identifiers. Tablet IDs must be positive integers returned by the accepted query plan. The `TABLET` clause appears before `WHERE`, matching Doris syntax.

The filter remains a trusted expression because validating statement boundaries isn't equivalent to SQL parameter binding.

## Classify failures at their source

Configuration, schema, planning, authentication, permission, and worker read failures have separate public exception types. Planning fallback catches only `DorisPlanningError` after excluding its authentication and permission subclasses.

Automatic transport fallback uses an internal setup-only exception. The Flight reader translates eligible dependency, I/O, timeout, and unsupported-operation failures before a reader exists. It doesn't translate stream and conversion errors into fallback signals.

## Define the consistency boundary

The connector doesn't coordinate a Doris transaction across tablet tasks. Metadata discovery, query planning, and split reads are separate requests. Task retry is at-least-once at the split level because a replacement task re-executes the complete split.

Applications that require a database snapshot or exactly-once downstream effects must establish those semantics outside `ray-doris`.

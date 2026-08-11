---
myst:
  html_meta:
    description: "ray-doris architecture, including documented Ray API adaptation, driver planning, schema mapping, Ray task construction, worker transports, and failure boundaries."
---

(ray-doris-architecture)=

# Architecture

`ray-doris` is a thin independent connector around Ray Data's documented V1 Datasource extension interfaces and Doris public protocols. Ray owns distributed scheduling and Dataset execution. Doris owns query semantics, tablet pruning, and storage replicas. The connector owns validation, protocol coordination, schema consistency, and failure classification. Ray marks `ReadTask` as DeveloperAPI, so `_compat.py` and the tested dependency window contain minor-version changes.

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

## Adapt documented Ray extension APIs

{ref}`read_doris <ray-doris-api-read-doris>` builds a datasource and calls `ray.data.read_datasource()` with `concurrency`, `override_num_blocks`, and a copied `ray_remote_args` mapping.

{ref}`DorisDatasource <ray-doris-api-datasource>` subclasses Ray's documented `Datasource` extension class. It returns DeveloperAPI `ReadTask` objects that yield PyArrow tables. `_compat.py` inspects the `ReadTask` constructor so it can reject unsupported Ray signatures without catching an unrelated `TypeError` from the read function.

## Plan on the driver

The driver performs two metadata operations. MySQL `DESCRIBE` produces column names, types, and nullability. An authenticated HTTP POST to `/_query_plan` produces predicate-pruned tablet IDs. When `password_env` is configured, each operation resolves the credential immediately before its own network request. A custom `http_ca_file` is loaded into a hostname-verifying TLS context for query planning.

The query-plan client disables redirects and validates both HTTP failures and Doris's body envelope. Doris can report application errors inside an HTTP 200 response, so HTTP status alone isn't enough.

Schema and tablet IDs form one immutable planning snapshot. `get_read_tasks()` reuses it when Ray requests tasks more than once, then groups the same tablet IDs using the new parallelism hint.

## Execute on workers

The MySQL reader distinguishes normal EOF from consumer abort. Normal completion closes the cursor and then the connection. Abort closes the connection first and detaches the active server-side cursor so PyMySQL can't drain the unread result. The Flight reader consumes RecordBatch objects, uses safe Arrow casts, verifies nullability, and slices tables into connector output batches. Both readers enforce the same result-column and non-nullable NULL contract.

MySQL is the production-candidate data path. Flight SQL and worker-local `auto` selection remain experimental and aren't part of a stable compatibility profile.

Transport option mappings are deep-copied into immutable tuple storage so caller mutation can't change a constructed datasource. Construction also verifies the actual configuration with Ray's worker serialization protocol and rejects unsupported values before network access. The configuration representation exposes option keys for diagnosis but redacts the password, password environment name, HTTP CA path, filter, and every option value. An environment credential is resolved per process and never written back into the serialized configuration.

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

The connector accepts one logical FE host and never discovers cluster members. Doris and the
deployment platform own FE election, quorum, health checking, and load-balancer failover. The
connector owns TLS verification and failure classification for the configured endpoint.

## Define the consistency boundary

The connector doesn't coordinate a Doris transaction across tablet tasks. Metadata discovery, query planning, and split reads are separate requests. Task retry is at-least-once at the split level because a replacement task re-executes the complete split.

Applications that require a database snapshot or exactly-once downstream effects must establish those semantics outside `ray-doris`.

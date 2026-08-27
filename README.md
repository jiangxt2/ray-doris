# ray-doris

`ray-doris` is an independent, community-maintained Apache Doris connector for Ray Data. It plans reads with
Doris FE's `_query_plan` endpoint, streams each tablet group through the MySQL protocol
or Arrow Flight SQL, and writes bounded batches through Doris HTTP Stream Load. The implementation uses Ray's documented Datasource and Datasink extension APIs and never
imports `ray.data._internal`. Ray marks `ReadTask` as DeveloperAPI, so the supported Ray window is
intentionally bounded and tested by minor release.

The project is alpha software. The tested compatibility window is:

| Python | Ray | Verification |
|---|---|---|
| 3.9 | 2.49.2 | Alpha legacy compatibility; documented ReadTask and public Datasink contracts |
| 3.10 | 2.57.0 | Unit, documented ReadTask, and public Datasink contract tests |
| 3.12 | 2.58.0 | Unit, documented/public contracts, and Doris 4.0.6 required IT |
| 3.13 | 2.58.0 | Unit, documented/public contracts, package installation, and import tests |

Doris 4.0.6 is the fixed compatibility target for both required and opt-in distributed integration
tests. The slow distributed suite is aligned to Ray 2.58.0. Enterprise-candidate releases require
one successful full run on the exact commit; this Alpha release does not claim that distributed
evidence or stable/production readiness.
This project is not maintained or endorsed by the Ray or Apache Doris projects.

## Documentation

The source documentation follows the Ray project structure and is built with Sphinx and MyST.
Start with the [documentation landing page](doc/source/index.md), then use the
[Quickstart](doc/source/quickstart.md), [user guides](doc/source/user-guide/index.md),
[API reference](doc/source/api/api.md), and [compatibility matrix](doc/source/compatibility.md).

Documentation claims are tied to the package source and test suites. The
[architecture guide](doc/source/architecture.md) distinguishes public contracts from internal
implementation details, and the [FAQ](doc/source/faq.md) records explicit scope limits.

## Installation

Install the default MySQL transport:

```bash
pip install ray-doris
```

Install optional Flight SQL support:

```bash
pip install "ray-doris[flight]"
```

Flight SQL requires Python 3.10 or newer because current ADBC Flight SQL releases no longer support
Python 3.9. Python 3.9 reached end of life on October 31, 2025. It remains an Alpha legacy
compatibility target for the default MySQL transport, not a stable or production profile.

The package accepts `ray[data]>=2.49.2,<2.59`. The runtime guard supports final releases in that
window and local rebuild suffixes such as `2.58.0+vendor.1`; release candidates, development
builds, and post-release builds aren't supported. Flight SQL and `transport="auto"` are
experimental; the MySQL protocol is the only production-candidate transport. Datasource
construction rejects an unsupported Ray release before opening a Doris connection.

## Quick start

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

`table` must be an exact `database.table` reference in Doris's internal catalog. External
catalog tables, joins, and multi-table query planning are not supported. `filter` is a trusted
SQL scalar expression: do not pass untrusted user input. Doris uses the `WHERE` expression for
tablet pruning, so the number of splits can change with the filter. A complex expression such as
a subquery can be executed through the configured single-task fallback, but it cannot use tablet
parallelism.

`tablet_size=1` provides the finest split granularity. Tables with thousands of tablets should
start with a larger value such as `tablet_size=32` and use `override_num_blocks` to tune Ray's
output blocks without creating one scheduling task per tablet.

## Write a Dataset

Writes use Ray's public `Datasink` API and Doris Stream Load. The facade returns connector-owned
statistics; Ray's `num_rows` and `size_bytes` remain separate accounting fields.

```python
import ray
from ray_doris import DorisConnection, DorisTable, write_doris

dataset = ray.data.from_items(
    [
        {"event_id": 1, "score": 95.0},
        {"event_id": 2, "score": 88.0},
    ]
)
result = write_doris(
    dataset,
    connection=DorisConnection(
        host="doris-fe.example.com",
        username="ray_writer",
        password_env="DORIS_PASSWORD",
        redirect_hosts=("doris-be.example.com",),
        redirect_ports=(8040,),
        redirect_policy="public",
    ),
    table=DorisTable("analytics", "events"),
    operation="load",
)
print(result.loaded_rows, result.batches)
```

`load` and `upsert` send bounded Parquet batches. `partial_update` sends line-delimited JSON and
requires a Merge-on-Write Unique Key table. Configure both a FE endpoint and an explicit,
certificate-validated FE-to-BE redirect allowlist for production. Write task retries are forced to
zero; a request whose final status is unknown raises an ambiguous-write error and is never replayed
under a new label. The connector does not provide DDL, overwrite/truncate, Stream Load 2PC,
whole-dataset atomicity, or exactly-once semantics.

Callers that construct `DorisDatasink` directly must pass `ray_remote_args={"max_retries": 0}` to
`Dataset.write_datasink()` themselves; only the `write_doris()` facade applies this policy
automatically.

## API

```python
read_doris(
    *,
    table,
    host,
    mysql_port=9030,
    http_port=8030,
    flight_port=8070,
    http_scheme="http",
    flight_scheme="grpc",
    user="root",
    password="",
    password_env=None,
    columns=None,
    filter=None,
    transport="mysql",
    on_query_plan_error="single_task",
    tablet_size=1,
    batch_size=10_000,
    connect_timeout=10.0,
    query_plan_timeout=None,
    http_ca_file=None,
    client_kwargs=None,
    flight_options=None,
    concurrency=None,
    override_num_blocks=None,
    ray_remote_args=None,
)
```

`password_env` stores an environment-variable name in the datasource and resolves its value before
each driver request and worker connection attempt. It is mutually exclusive with a non-empty
`password`. The variable name and resolved value are redacted from representations; the resolved
value is never stored in the serialized datasource or ReadTask.

`http_scheme` accepts `http` or `https`. HTTPS requires an HTTPS endpoint, commonly a TLS reverse
proxy in front of the Doris FE HTTP API. Set `http_ca_file` for a private CA; hostname verification
remains enabled. `flight_scheme` accepts `grpc` or `grpc+tls`; configure certificates and other ADBC
settings with `flight_options`.

`connect_timeout` is passed to each PyMySQL connection attempt and the ADBC Flight SQL connect RPC.
`query_plan_timeout` controls the `_query_plan` HTTP request and defaults to `connect_timeout` when
unset. Neither value sets a deadline for an established MySQL socket read or a Flight SQL
query/fetch RPC.
Configure those limits explicitly when required:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    transport="mysql",
    connect_timeout=10.0,
    query_plan_timeout=30.0,
    client_kwargs={"read_timeout": 300, "write_timeout": 30},
    flight_options={
        "adbc.flight.sql.rpc.timeout_seconds.query": "300",
        "adbc.flight.sql.rpc.timeout_seconds.fetch": "300",
    },
)
```

`client_kwargs` otherwise contains PyMySQL connection options such as TLS configuration. Option
mappings are defensively copied. `connect_timeout` and its deprecated `passwd` credential alias
cannot override managed options. `read_timeout` and `write_timeout` must be finite positive numbers.

`tablet_size` is a soft tablet grouping target. `batch_size` is a hard row-fetch bound for MySQL
and a hard row bound for Arrow blocks emitted to Ray by Flight. It cannot constrain the size of a
RecordBatch already produced or prefetched by Doris and ADBC. `concurrency` limits simultaneous
Ray read tasks, while `override_num_blocks` controls Ray's output block planning.

The function returns a `ray.data.Dataset`. Configuration failures raise `DorisConfigurationError`,
unsupported schemas raise `DorisSchemaError`, and query planning failures raise
`DorisPlanningError`. Authentication and authorization failures use the more specific
`DorisAuthenticationError` and `DorisPermissionError`. Worker transport and conversion failures
raise `DorisReadError`. Explicit Flight without its optional dependency raises `ImportError` with
an installation command.

## Transports

The default `transport="mysql"` uses a PyMySQL server-side cursor and `fetchmany()`; it never
materializes the complete result in the worker. `transport="flight"` is experimental, requires the
Flight extra, and fails with an installation hint if it is missing. Flight RecordBatches are
streamed and sliced into Ray blocks of at most `batch_size` rows, but their server-side size and
ADBC prefetch memory are controlled by Doris and ADBC.

The experimental `transport="auto"` mode attempts Flight only when the extra is available in the
execution environment.
It falls back to MySQL only
when dependency loading, connection creation, cursor creation, query setup, or protocol negotiation
fails before rows are produced. An execute/fetch timeout does not fall back because MySQL might not
have an equivalent execution deadline; an unsupported Flight operation may fall back. Selecting
`flight_scheme="grpc+tls"` makes Flight setup fail closed instead of downgrading to MySQL. Once a
Flight reader exists, stream, SQL, schema, authentication, and permission errors never trigger
transport fallback.

Doris schema discovery always uses `DESCRIBE` through the MySQL port. Supported scalar types are
mapped to an explicit Arrow schema. Nested and aggregate-state types fail closed rather than being
silently stringified. Decimal precision up to 38 uses Arrow decimal128 and precision up to 76 uses
decimal256; `DATETIMEV2` is represented as `timestamp[us]`.

## Query-plan failures

Doris normally returns HTTP 200 even for `_query_plan` errors. `ray-doris` classifies the Doris
body envelope (`code`, inner `status`, and `exception`) rather than assuming that HTTP status is
the application result. Invalid credentials and `Access denied` responses fail immediately.
Other planning failures follow `on_query_plan_error`:

- `single_task` (default) removes the `TABLET` hint and executes one query;
- `error` raises `DorisPlanningError`.

Successful predicate pruning with an empty `partitions` object is a valid empty read and does not
fall back to a full-table query. Ray receives a schema-carrying zero-row block so `Dataset.schema()`
remains available.

## Advanced Ray usage

`DorisDatasource` is part of this package's public surface for callers that need to invoke
`ray.data.read_datasource()` directly. Ray's `ReadTask` remains DeveloperAPI. Only pass keyword
arguments documented by your installed Ray version to that function. The
convenience `read_doris()` entry point exposes the common cross-version arguments
`concurrency`, `override_num_blocks`, and `ray_remote_args`; unknown keyword arguments fail fast.

Ray may call `get_read_tasks()` more than once while constructing one read, so each
`DorisDatasource` instance caches the schema and tablet discovery result from its first planning
call. Treat an instance as one logical read and create a new instance to discover table or tablet
changes made later. This planning cache does not provide snapshot isolation.

Ray serializes datasource configuration to workers. A literal `password` therefore remains in task
state for compatibility and is suitable only for a trusted cluster. The enterprise-candidate MySQL
profile uses `password_env`, injects the same variable into the driver and every Ray worker, and
resolves it separately for each request or connection attempt without serializing the value.
Transport option values are redacted from representations and logs but remain serialized, so TLS
paths and other sensitive option values still require a trusted Ray control plane and object store.

Configure MySQL TLS through `client_kwargs`; set `http_scheme="https"` and `http_ca_file` for a
protected query-plan endpoint. The enterprise-candidate profile also uses
`on_query_plan_error="error"`, explicit query-plan/MySQL timeouts, and a minimum-privilege reader.
The defaults are unencrypted and must only be used on a trusted private network. Flight TLS remains
deployment-specific and experimental.

The Doris reader account needs access to the FE MySQL and HTTP ports and `SELECT` on the target
internal-catalog table. Flight reads additionally need the FE Flight SQL port. The `_query_plan`
endpoint itself performs the table authorization check.

Tablet planning and task execution do not provide snapshot isolation. Concurrent Doris writes can
therefore produce a result that reflects different moments across splits. If a Ray task fails after
reading part of a split, Ray can retry the whole task; the connector does not resume a partial split.

Configure one logical FE hostname that is valid for both HTTPS and MySQL TLS. `ray-doris` validates
and uses that endpoint but doesn't discover FE members or implement leader election, quorum, health
checks, or cross-endpoint failover. Production deployments must provide and validate those HA
properties in Doris and their external load balancer.

The required Doris 4.0.6 integration suite uses the default HTTP endpoint. The distributed suite
uses the same fixed Doris version, validates native MySQL TLS, and validates certificate-checked
HTTPS through an HAProxy ingress that forwards to the FE HTTP endpoint. It does not enable Doris
4.0.6 native FE HTTPS because that release has a Jetty WebSocket startup regression. Doris 4.0.6
advertises plaintext Flight `grpc` endpoints rather than native Flight TLS endpoints, so the suite
keeps Flight on an isolated Compose network and does not claim a positive `grpc+tls` server test.
Deployments that provide a compatible Flight TLS endpoint must validate their certificates and ADBC
options separately.

## Troubleshooting

- `DorisAuthenticationError`: verify the same credentials work on both the FE MySQL and query-plan
  endpoints. An HTTPS proxy must forward the `Authorization` header.
- `DorisPermissionError`: grant `SELECT_PRIV` on the internal-catalog table. The connector does not
  use administrator-only `SHOW TABLETS` fallback.
- `DorisPlanningError`: use `on_query_plan_error="single_task"` for a trusted complex filter that
  Doris can execute but `_query_plan` cannot represent, or use `error` to diagnose the body status.
- Flight connection failures: verify the FE and BE Flight ports, URI scheme, certificates, and ADBC
  options. Explicit Flight never silently switches transports.
- Doris zero dates such as `0000-00-00` cannot be represented losslessly as Arrow date or timestamp
  values. Reads fail closed and identify the affected column; values are not coerced to null.
- Query-plan redirects are rejected because redirecting an authenticated POST can change its method
  or expose credentials. Configure the final HTTP/HTTPS endpoint directly.
- Slow or hung reads: `connect_timeout` covers planning HTTP requests, MySQL connection setup, and
  the Flight connect RPC, but not query execution. Configure PyMySQL `read_timeout`/`write_timeout`
  or the ADBC Flight SQL query/fetch timeout options when an execution deadline is required.
- Schema failures: project only supported scalar columns; nested and aggregate-state types fail
  closed by design.

## Development

Create the environment and run the unit gate:

```bash
uv venv --python 3.12
uv pip install -e ".[dev,flight]"
.venv/bin/ruff format --check .
.venv/bin/ruff check .
.venv/bin/mypy
.venv/bin/python -m pytest tests/unit
.venv/bin/python -m pytest tests/contract
```

Run the required real Doris integration suite:

```bash
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -f tests/integration/docker-compose.yml build fe
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -f tests/integration/docker-compose.yml build be
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -f tests/integration/docker-compose.yml up -d --no-build
.venv/bin/python -m pytest tests/integration
env -u HTTP_PROXY -u HTTPS_PROXY -u ALL_PROXY \
  -u http_proxy -u https_proxy -u all_proxy \
  docker compose -f tests/integration/docker-compose.yml down -v --rmi local
```

The integration fixture can use an existing isolated Doris instance when the `DORIS_*` connection
variables are set. It creates and removes only the `ray_doris_it` database and its test users. It
also configures the isolated BE public endpoint required for Doris 4.x Stream Load redirects.

### Slow distributed integration

The opt-in slow suite runs the following isolated topology:

- one Ray head with no scheduling CPUs and three one-CPU Ray workers;
- one Doris 4.0.6 FE and three Doris 4.0.6 BEs;
- one HAProxy ingress for HTTPS, FE MySQL/Flight routing, BE Flight routing, and certificate-checked
  BE Stream Load routing through Doris public endpoints;
- a 48-tablet, single-replica table distributed across all BEs;
- a 48-tablet, three-replica table used for BE failure recovery;
- certificate-verified HTTPS query planning at the ingress and native Doris MySQL TLS;
- explicit Arrow Flight SQL reads, with no automatic MySQL fallback;
- per-BE Flight session and byte counters proving that all three BE services receive traffic;
- a minimum-privilege MySQL read distributed across all three Ray workers;
- a bounded Parquet Stream Load write distributed across all three Ray workers with Doris readback;
- a deterministic post-send Stream Load transport fault executed by a Ray write task and classified
  as ambiguous without replay;
- a Ray worker failure after the first MySQL block and a complete-split retry on another worker;
- a Doris BE failure and a complete MySQL read from surviving replicas;
- 10,000 rows by default and repeated checksum-validated Flight reads for at least five seconds.

It is excluded from the default pytest discovery paths and from the regular CI workflow. Run it
manually on a Docker host with at least 16 GiB of available memory:

```bash
tests/slow_integration/run.sh
```

For a Core read hardening change that affects only task count or worker distribution, run the
targeted profile without the worker/BE failure scenarios:

```bash
RAY_DORIS_SLOW_PROFILE=core tests/slow_integration/run.sh
```

The script automatically reuses a local `ray-cluster:2.58.0` image when present. Otherwise, the
Dockerfile uses the fixed public base `rayproject/ray:2.58.0-py312-cpu`. You can select another
trusted local image with `RAY_BASE_IMAGE`; the build verifies `ray.__version__` before installing
this project:

```bash
RAY_BASE_IMAGE=ray-cluster:2.58.0 tests/slow_integration/run.sh
```

The default profile is a functional distributed integration test, not a load test. On a dedicated
host, use `RAY_DORIS_ROW_COUNT`, `RAY_DORIS_STRESS_SECONDS`, and
`RAY_DORIS_BE_MEMORY_LIMIT` to opt into a larger load or extended soak. For example:

```bash
RAY_DORIS_ROW_COUNT=1000000 \
RAY_DORIS_STRESS_SECONDS=300 \
RAY_DORIS_BE_MEMORY_LIMIT=4g \
tests/slow_integration/run.sh
```

Size the dedicated host for the requested container limits. The script refuses to reuse an
existing `ray-doris-it` Compose project, preserves pytest, Ray, Doris, and HAProxy logs, and removes
only the resources created by that exact project.

Successful `full` runs write a schema-versioned `slow-result.json` containing the tested commit,
workflow run, Ray/Doris/Python versions, image IDs, topology, parameters, and scenario list. The
workflow uploads it under an artifact name bound to the tested commit; release verification accepts
only one successful, non-expired full manifest whose commit and workflow run ID exactly match the
downloaded artifact source. The dedicated runner must provide Linux x64, Docker Compose, at least
16 GiB available memory, at least 20 GiB free disk, and Actions Runner 2.327.1 or newer.
Register it with the `ray-doris-slow-it` and `ray-doris-slow-it-node24` labels.
Enterprise release profiles require the full manifest. Alpha release profiles may defer the slow
evidence, but must not claim enterprise or stable distributed certification. Formal recovery of the
immutable `v1.0` release remains a separate historical exception.

See [CONTRIBUTING.md](CONTRIBUTING.md) for the complete checks.

Build and validate the documentation with the commands in the
[documentation contributor guide](doc/source/contributing/index.md).

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). Version history is published through
[GitHub Releases](https://github.com/jiangxt2/ray-doris/releases) from the versioned files under
`release-notes/`.

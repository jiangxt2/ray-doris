---
myst:
  html_meta:
    description: "Choose the ray-doris MySQL, Arrow Flight SQL, or auto transport and configure connection and execution timeouts without unsafe fallback."
---

(ray-doris-configure-transports)=

# Configure transports

`ray-doris` uses MySQL for schema discovery and supports MySQL or Arrow Flight SQL for split data. The transport choice changes worker data movement, not driver-side `DESCRIBE` or HTTP tablet planning.

## Compare transport modes

Choose one of three transport values:

| Mode | Behavior |
| --- | --- |
| `mysql` | Uses a PyMySQL server-side cursor and bounded `fetchmany()` calls. This is the default. |
| `flight` | Requires the Flight extra and streams Arrow RecordBatch objects through Arrow Database Connectivity (ADBC) Flight SQL. Setup or read failures don't switch to MySQL. |
| `auto` | Attempts Flight when its dependencies exist in the worker environment and falls back only for eligible setup or protocol-negotiation failures before Flight produces rows. |

From a source checkout, install the Flight dependency in every execution environment that can run a Ray read task:

```bash
uv pip install -e ".[flight]"
```

Flight SQL requires Python 3.10 or newer. Explicit Flight on Python 3.9 raises `ImportError` with an installation or version hint.

## Stream with MySQL

The MySQL reader uses `pymysql.cursors.SSCursor` and yields one Arrow table per `fetchmany(batch_size)` result:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    transport="mysql",
    batch_size=10_000,
    client_kwargs={"read_timeout": 300, "write_timeout": 30},
)
```

`read_timeout` and `write_timeout` must be finite positive numbers. `client_kwargs` can't replace connector-managed values such as host, port, database, user, password, charset, cursor class, or connect timeout.

## Stream with Flight SQL

The Flight reader obtains an ADBC RecordBatch reader, casts each column safely to the planned schema, and slices each produced table into blocks with at most `batch_size` rows:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    transport="flight",
    flight_port=8070,
    batch_size=10_000,
    flight_options={
        "adbc.flight.sql.rpc.timeout_seconds.query": "300",
        "adbc.flight.sql.rpc.timeout_seconds.fetch": "300",
    },
)
```

`batch_size` doesn't constrain a RecordBatch that Doris or ADBC already produced or prefetched. Configure server-side batches and ADBC memory behavior outside this connector when your deployment exposes those controls.

## Understand automatic fallback

The connector limits automatic fallback to failures that occur before a usable Flight reader produces rows. Missing dependencies, connection creation, cursor creation, eligible query setup errors, I/O, and unsupported protocol operations can select MySQL.

An execute or fetch timeout doesn't fall back because MySQL might not have an equivalent execution deadline. Schema mismatch, authentication, permission, conversion, and stream errors don't fall back. Once Flight yields a row batch, any later error terminates the split instead of starting the query again through MySQL.

Setting `flight_scheme="grpc+tls"` makes `auto` fail closed. A TLS setup failure never downgrades to an unencrypted MySQL read.

## Configure timeout scopes

`connect_timeout` applies to three setup operations:

- The frontend `_query_plan` HTTP request.
- Each PyMySQL connection attempt.
- The ADBC Flight SQL connect remote procedure call (RPC).

It doesn't set a deadline for an established MySQL socket read or a Flight query and fetch. Configure those execution deadlines with `client_kwargs` and `flight_options`:

```python
dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    transport="auto",
    connect_timeout=10.0,
    client_kwargs={"read_timeout": 300, "write_timeout": 30},
    flight_options={
        "adbc.flight.sql.rpc.timeout_seconds.query": "300",
        "adbc.flight.sql.rpc.timeout_seconds.fetch": "300",
    },
)
```

The datasource constructor copies option mappings. Their keys appear in the redacted configuration representation, but their values don't.

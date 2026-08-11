---
myst:
  html_meta:
    description: "Copy source-aligned ray-doris examples for projected MySQL reads, explicit Flight SQL, secure endpoints, empty results, and direct Datasource use."
---

(ray-doris-examples)=

# Examples

These examples require a reachable Apache Doris deployment. The documentation check compiles every Python block, while the repository's integration suites execute equivalent behaviors against Doris 4.0.6.

## Run the source-controlled quickstart

The repository example reads connection settings from environment variables:

```{literalinclude} ../../../examples/quickstart.py
:language: python
```

## Read through Flight SQL

Flight SQL is experimental. Install the Flight extra in every Ray worker environment, then select the transport explicitly:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    user="ray_reader",
    password="...",
    columns=["event_id", "created_at", "score"],
    filter="score >= 80",
    transport="flight",
    flight_port=8070,
    tablet_size=16,
    batch_size=5_000,
    concurrency=4,
    override_num_blocks=32,
)

rows = dataset.take(10)
```

Explicit Flight never switches to MySQL after a failure.

## Protect MySQL and query planning

Use independent TLS settings for the MySQL and HTTP endpoints:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris.example.com",
    mysql_port=9030,
    http_port=8443,
    http_scheme="https",
    http_ca_file="/etc/doris-tls/ca.pem",
    user="ray_reader",
    password_env="DORIS_PASSWORD",
    query_plan_timeout=30,
    on_query_plan_error="error",
    client_kwargs={
        "ssl": {
            "ca": "/etc/doris-tls/ca.pem",
            "check_hostname": True,
        },
        "read_timeout": 300,
        "write_timeout": 30,
    },
)
```

The default Flight `grpc` scheme remains unencrypted unless you configure a compatible `grpc+tls` endpoint.

## Preserve an empty schema

A successfully pruned empty read keeps the projected schema:

```python
from ray_doris import read_doris

dataset = read_doris(
    table="analytics.events",
    host="doris-fe.example.com",
    columns=["event_id", "score"],
    filter="1 = 0",
)

assert dataset.take_all() == []
assert dataset.schema().names == ["event_id", "score"]
```

## Configure the datasource directly

Use the class API when you need direct control over `ray.data.read_datasource()`:

```python
import ray.data

from ray_doris import DorisDatasource

datasource = DorisDatasource(
    table="analytics.events",
    host="doris-fe.example.com",
    transport="mysql",
    tablet_size=8,
    batch_size=10_000,
)

dataset = ray.data.read_datasource(
    datasource,
    concurrency=4,
    override_num_blocks=32,
    ray_remote_args={"num_cpus": 1, "max_retries": 3},
)
```

Pass only options supported by your installed Ray version.

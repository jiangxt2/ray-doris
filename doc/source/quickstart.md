---
myst:
  html_meta:
    description: "Install ray-doris from source, connect Ray Data to an Apache Doris internal-catalog table, and inspect the resulting Dataset."
---

(ray-doris-quickstart)=

# Quickstart

This guide reads a projected Doris table into Ray Data with the default MySQL transport.

## Prepare the environment

Use Python 3.12 for the same environment as the required Doris integration suite. Clone the repository and install the project in an isolated environment:

```bash
git clone https://github.com/jiangxt2/ray-doris.git
cd ray-doris
uv venv --python 3.12
uv pip install -e .
```

Install the optional Flight SQL dependency when you plan to use `transport="flight"` or `transport="auto"`:

```bash
uv pip install -e ".[flight]"
```

Flight SQL requires Python 3.10 or newer. The default MySQL transport remains available on Python 3.9.

Flight SQL and `auto` are experimental. Use the default MySQL transport for the production-candidate profile. Python 3.9 is retained only as an Alpha legacy compatibility target because it no longer receives upstream security fixes.

## Prepare Doris access

Create or select an internal-catalog table and grant the reader account `SELECT_PRIV` on that table. The account must reach the Doris frontend MySQL and HTTP ports. A Flight read also requires access to the frontend Flight SQL port and the backend Flight endpoints advertised by Doris.

The examples use this logical table:

```sql
CREATE TABLE `analytics`.`events` (
    `event_id` BIGINT NOT NULL,
    `created_at` DATETIMEV2(6) NULL,
    `score` DOUBLE NULL
)
ENGINE=OLAP
DUPLICATE KEY(`event_id`)
DISTRIBUTED BY HASH(`event_id`) BUCKETS 4
PROPERTIES ("replication_num" = "1");
```

## Run the example

Set connection values in the environment, then run the source-controlled quickstart:

```bash
export DORIS_TABLE=analytics.events
export DORIS_HOST=127.0.0.1
export DORIS_MYSQL_PORT=9030
export DORIS_HTTP_PORT=8030
export DORIS_HTTP_SCHEME=http
export DORIS_USER=ray_reader
export DORIS_PASSWORD=<password>
python examples/quickstart.py
```

The example passes `password_env="DORIS_PASSWORD"`; it doesn't read the value into datasource
configuration. In a cluster, inject that variable into the driver and all Ray workers.

The example uses the public {ref}`read_doris <ray-doris-api-read-doris>` entry point:

```{literalinclude} ../../examples/quickstart.py
:language: python
```

`dataset.schema()` exposes the canonical Arrow schema planned on the driver. `dataset.take(5)` starts execution and returns up to five rows from the distributed Ray Dataset.

## Continue learning

See [Key concepts](key-concepts.md) before tuning task counts. Then use the [read data](user-guide/read-data.md), [write data](user-guide/write-data.md), [transport](user-guide/configure-transports.md), and [parallelism](user-guide/tune-parallelism.md) guides for production configuration.

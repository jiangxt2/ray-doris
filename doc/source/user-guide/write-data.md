---
myst:
  html_meta:
    description: "Write Ray Data batches to Apache Doris with Stream Load, table-model validation, bounded serialization, strict redirects, and fail-closed retry semantics."
---

(ray-doris-write-data)=

# Write data

Use `write_doris()` for the normal facade or construct `DorisDatasink` when the Ray
`Dataset.write_datasink()` lifecycle must be controlled directly. Both paths use the public Ray
Datasink API and HTTP Stream Load; the connector does not issue row-by-row SQL inserts.

## Choose an operation

| Operation | Doris table requirement | Payload |
| --- | --- | --- |
| `load` | Duplicate, Unique, or Aggregate Key | Parquet, complete table columns in physical order |
| `upsert` | Unique Key | Parquet, complete table columns in physical order |
| `partial_update` | Merge-on-Write Unique Key | Line-delimited JSON, key columns plus a known column subset |

The connector discovers `SHOW CREATE TABLE` and `DESCRIBE` metadata before the first request. It
does not create or alter tables. Full-row operations reject missing, extra, reordered, duplicate,
or unsafe columns. Partial updates must include every key column and cannot target a non-Merge-on-
Write Unique Key table.

## Configure a bounded write

```python
import ray
from ray_doris import DorisConnection, DorisTable, write_doris

dataset = ray.data.from_items(
    [
        {"id": 1, "value": "first"},
        {"id": 2, "value": "second"},
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
    batch_rows=65_536,
    batch_bytes=64 * 1024 * 1024,
)
assert result.loaded_rows == 2
```

`batch_rows` and `batch_bytes` are serialization targets. `DorisDatasink.min_rows_per_write` returns
the row target so Ray normally bundles an input close to one connector batch, but the property is
not a strict physical request boundary. A single row larger than the byte target is sent as its own
batch.

## Use partial updates

```python
result = write_doris(
    ray.data.from_items([{"id": 1, "value": "changed"}]),
    connection=connection,
    table=DorisTable("analytics", "events"),
    operation="partial_update",
)
```

The server applies Merge-on-Write semantics to omitted columns. The connector does not emulate
this behavior in Ray and does not send a partial update to a table whose metadata cannot prove the
required model and key contract.

## Understand results and retries

`DorisWriteResult` exposes `status`, physical `batches`, attempted/loaded/filtered row counts, and
uploaded bytes from sanitized task summaries. Its `ray_num_rows` and `ray_size_bytes` fields come
from Ray's `WriteResult` accounting and are intentionally separate from Doris counters.

The facade forces Ray write task `max_retries=0` and rejects equivalent exception-retry settings.
When constructing `DorisDatasink` directly, pass `ray_remote_args={"max_retries": 0}` to every
`Dataset.write_datasink()` call yourself; the sink cannot rewrite Ray task options after it has been
constructed. Do not enable exception retries on the direct path.
If the request body may have reached Doris but its outcome is unknown, the connector raises
`DorisAmbiguousWriteError` and never replays the payload under a new label. `Publish Timeout` is
reported explicitly and is not retried. `Label Already Exists`, malformed responses, and known
Stream Load failures are fail-closed errors.

This profile provides neither exactly-once behavior nor whole-dataset atomicity. Each Stream Load
request is an independent Doris batch; applications requiring reconciliation must use their own
authoritative Doris status and operational workflow.

## Secure the write path

Use `password_env` for distributed credentials. Set `http_secure=True` and `http_ca_file` for HTTPS
Stream Load, and set `mysql_ca_file` when metadata discovery must use MySQL TLS. `redirect_hosts` and
`redirect_ports` are an explicit allowlist; userinfo in a Doris `Location` is discarded, and an HTTPS
connection cannot downgrade to HTTP. Environment proxies are not used for Stream Load.

The writer needs MySQL permission for `SHOW CREATE TABLE` and `DESCRIBE`, plus Doris Stream Load
permission on the target table. The test profile configures Doris BE `tag.public_endpoint` values so
FE redirects resolve to an address reachable from every Ray worker.

See [Secure connections](secure-connections.md) for the shared credential boundary and
[Troubleshoot writes](troubleshooting.md#troubleshoot-writes) for error categories.

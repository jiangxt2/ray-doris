---
myst:
  html_meta:
    description: "Diagnose ray-doris configuration, authentication, permission, planning, schema, transport, write, timeout, and empty-result behavior."
---

(ray-doris-troubleshooting)=

# Troubleshoot reads and writes

Start with the public exception type and the stage named in its message. Connector exceptions include table or tablet context without including passwords or option values.

## Fix configuration errors

{ref}`DorisConfigurationError <ray-doris-api-configuration-error>` means local validation failed before a data read. Check these inputs:

- `table` uses exact `database.table` form.
- Host and user values aren't empty.
- Ports are integers from 1 through 65535.
- `tablet_size`, `batch_size`, and Ray parallelism values are positive integers.
- Timeout values are finite and positive.
- Option mappings don't override connector-managed connection fields.
- `password_env` is a portable environment name and is available in both driver and worker processes.
- `http_ca_file` is non-empty and used only with `http_scheme="https"`.

An HTTPS certificate failure, a MySQL TLS setup failure during driver planning, and an authenticated
query-plan redirect also use this exception because you must change the configuration before
planning can continue safely. Messages don't include environment-variable names, CA paths, or
driver exception text.

## Fix authentication and permission errors

{ref}`DorisAuthenticationError <ray-doris-api-authentication-error>` identifies rejected credentials, including MySQL error 1045 and query-plan body or HTTP authentication failures. Verify the same account against both the frontend MySQL port and query-plan endpoint. An HTTPS proxy must forward the `Authorization` header.

{ref}`DorisPermissionError <ray-doris-api-permission-error>` identifies missing access, including MySQL permission codes and the query-plan service's `Access denied` response. Grant `SELECT_PRIV` on the exact internal-catalog table.

Neither exception uses planning fallback.

## Diagnose planning failures

{ref}`DorisPlanningError <ray-doris-api-planning-error>` can indicate an unavailable query-plan endpoint, invalid JSON, a malformed Doris response envelope, invalid tablet IDs, schema discovery failure, or a Doris planning rejection.

Use `on_query_plan_error="error"` to inspect a rejection without single-task fallback. Use `single_task` only with a trusted filter that Doris can execute as one query.

If `filter="1 = 0"` returns no rows, that's a valid empty plan rather than a failure. The Dataset retains its schema.

## Diagnose schema failures

{ref}`DorisSchemaError <ray-doris-api-schema-error>` identifies an invalid `DESCRIBE` result, an unknown projected column, an invalid decimal declaration, or an unsupported Doris type. See [Work with schemas](work-with-schemas.md) and project only supported scalar fields.

## Diagnose worker read failures

{ref}`DorisReadError <ray-doris-api-read-error>` identifies a split failure after planning. Check the transport named in the message, the tablet context, Doris frontend and backend logs, network reachability from the Ray worker, and the configured execution timeouts.

A Flight stream, schema, authentication, permission, or conversion error doesn't trigger automatic MySQL fallback. This prevents duplicate rows after Flight has started returning data.

## Diagnose slow or stalled reads

### Enable connector diagnostics

The application owns logging levels, handlers, formatting, and Ray worker setup.
The connector does not configure the root logger or install a worker hook.
Setting a logger level in the driver does not update existing worker processes.

For an independent application, configure Ray before creating the Dataset:

```python
import logging
import os
import ray

os.environ["RAY_DORIS_LOG_BATCHES"] = "1"
ray.init(
    logging_config=ray.LoggingConfig(encoding="TEXT", log_level="DEBUG"),
    runtime_env={"env_vars": {"RAY_DORIS_LOG_BATCHES": "1"}},
)
logging.getLogger("ray_doris").setLevel(logging.DEBUG)
```

Ray's text and JSON logging configuration includes custom event fields. For a
custom application formatter, explicitly include `event`, `phase`, `status`,
`operation_id`, and available `plan_id`, `split_id`, `execution_id`, and `label`
fields. Use defaults for fields that do not apply to every event. For example,
this application formatter works with Python 3.9 and newer:

```python
class ConnectorFormatter(logging.Formatter):
    def format(self, record):
        keys = ("event", "phase", "status", "operation_id", "execution_id", "label")
        return " ".join(f"{key}={getattr(record, key, '-')}" for key in keys)
```

When choosing package-specific output, install the application's formatter and
handler in both the driver and its own `worker_process_setup_hook` before
starting Ray. A Ray Jobs submission uses a fully qualified, importable setup
function, such as `my_application.logging_setup.configure`. Workers must be able
to import that package before executing tasks; install it or
include it in the job runtime environment. An existing
Ray session keeps its original worker configuration. A Tributo application
configures the `ray_doris` namespace through its application logging policy;
the connector also works independently of Tributo and does not send telemetry.

`RAY_DORIS_LOG_BATCHES` accepts only `0` or `1`, defaults to `0`, and is frozen
when constructing a datasource or sink. Invalid values fail before network I/O.
Changing a worker's environment or changing the environment after construction
does not override that snapshot. This option does not change logging levels:
batch progress requires both the option and DEBUG output at the logger and handler.

Progress records the first eligible batch, then batches divisible by 100 when
at least one second has elapsed since the last record, with at most 100 records
per physical execution. Failures, uncertain outcomes, cleanup warnings, and
stage boundaries are independent of that budget. The first load request has
start and finish events even when batch progress is disabled; later successful
requests are sampled. Logging still follows the application's level and filters.

`operation_id` identifies the logical read or write. Repeated planning retains
that operation and gets a new `plan_id`; each split has a `split_id`. Each physical
read or worker write has a new `execution_id`, including a retried read. A read
retry restarts the entire split. Planning fallback changes tablet grouping to
one unpartitioned task; transport fallback changes an eligible Flight setup to
MySQL. These are separate events and do not introduce new fallback behavior.

Discovery, preparation, and first-request start events help identify a blocked
stage. A missing finish event can also mean process termination. Driver write
failure callbacks are best effort and omit unknown global totals. A failed
Dataset can have successful requests; `Publish Timeout` does not mean rollback.
Failed worker totals, when available, describe only previously confirmed batches
(`counts_scope=confirmed_batches`); they do not establish the final outcome of
the failing request. No response means those counters are omitted.
Use an exceptional load's generated `label` to investigate its existing request,
and do not replay an ambiguous write automatically.

Rows count connector output or validated Doris response counters, rather than
consumer processing. Read elapsed time includes generator pauses and backpressure.
`arrow_bytes` describes the observed Arrow block, while `uploaded_bytes` counts
serialized request bytes. Empty streams do not have a first nonempty block event.
Events exclude credentials, credential environment names, SQL, filters, paths,
option mappings, raw exception chains, and business rows. Experimental native
Hive Parquet reads report selection and metadata counts; Ray owns file reading
statistics, and logging does not change native support or release status.

`query_plan_timeout` bounds query-plan request I/O. `connect_timeout` doesn't stop an established
query. Configure PyMySQL `read_timeout` and `write_timeout` or Arrow Database Connectivity (ADBC)
Flight query and fetch timeouts. Then compare Ray task duration with Doris query and backend metrics.

Reduce `concurrency` when Doris carries too much load. Increase `tablet_size` when Ray schedules too many short tasks. Don't interpret `override_num_blocks` as a Doris connection limit.

## Report an issue safely

Include this information:

- Package, Python, Ray, and Doris versions.
- Transport and exception class.
- Redacted traceback.
- Table schema without sensitive names when possible.
- Minimal reproduction.

Never include passwords, TLS private keys, authorization headers, or full option values.

Report suspected vulnerabilities through the repository's private GitHub security advisory interface, as described in `SECURITY.md`.

(ray-doris-troubleshoot-writes)=

## Troubleshoot writes

`DorisMetadataError` means `SHOW CREATE TABLE` or `DESCRIBE` was missing, malformed, or inaccessible.
`DorisTableCompatibilityError` means the operation, model, key columns, nullability, type, or full-
row column order could not be validated before upload. Check the table with the same account used by
the Ray workers; the connector does not create or alter the table.

`DorisWriteError` identifies a known Stream Load failure. `DorisLabelExistsError` means Doris retained
the generated label and the connector deliberately does not guess whether it belongs to this write.
`DorisAmbiguousWriteError` means the request body may have reached Doris but the final outcome is
unknown. Do not rerun the Dataset automatically under a new label; reconcile the target table using
an application-owned workflow.

For a redirect failure, verify that Doris BE nodes have a reachable `tag.public_endpoint`, that the
returned scheme is HTTPS when `http_secure=True`, and that every redirected host and port is in the
explicit allowlist. For `Publish Timeout`, inspect Doris load visibility before any manual decision;
the connector reports the state and does not replay the batch.

`write_doris()` rejects non-zero Ray task retries because Stream Load is an external side effect.
`DorisWriteResult.attempted_rows`, `loaded_rows`, `filtered_rows`, `batches`, and `uploaded_bytes`
come from sanitized Doris task summaries. `ray_num_rows` and `ray_size_bytes` are Ray accounting and
must not be interpreted as Doris loaded counters.

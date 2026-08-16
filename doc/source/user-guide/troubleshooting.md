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

---
myst:
  html_meta:
    description: "Diagnose ray-doris configuration, authentication, permission, planning, schema, transport, timeout, and empty-result behavior."
---

(ray-doris-troubleshooting)=

# Troubleshoot reads

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

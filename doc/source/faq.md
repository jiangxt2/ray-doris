---
myst:
  html_meta:
    description: "Answers to common ray-doris questions about writes, catalogs, SQL filters, schema discovery, planning cache, fallback, retries, and installation."
---

(ray-doris-faq)=

# FAQ

This page answers common scope and behavior questions. Use [Troubleshoot reads](user-guide/troubleshooting.md) when a specific operation fails.

## Can ray-doris write to Doris?

No. The package implements bounded batch reads into Ray Data. It doesn't implement a Ray Datasink, Doris Stream Load, insert operations, table creation, or schema changes.

## Can it read an external catalog table?

No. `table` accepts one exact internal-catalog `database.table` reference. A three-part catalog name fails local validation before any Doris connection.

## Can it execute arbitrary SQL?

No. The connector generates `SELECT <projection> FROM <table>` and optionally adds a trusted `WHERE` expression and Doris `TABLET(...)` hint. It doesn't accept a full query string, join, or multi-table plan.

A trusted complex filter can execute through the single-task planning fallback when Doris accepts the complete generated statement but `_query_plan` can't produce tablet splits.

## Is filter safe for request parameters?

No. `filter` is a trusted SQL expression. The connector rejects separators, comment tokens outside literals, and unterminated quotes, but it doesn't parse or parameterize values. Build request-facing predicates with an application-owned validation and binding layer before calling `read_doris()`.

## Why does Flight still require the MySQL port?

Schema discovery always uses MySQL `DESCRIBE`. Flight SQL moves split data after the driver has built the canonical Arrow schema. The HTTP endpoint remains necessary for tablet planning.

## Why did auto use MySQL?

`auto` is experimental. It selects MySQL when Flight dependencies are missing or an eligible Flight setup or protocol-negotiation failure occurs before Flight produces rows. Enable connector logging and inspect worker dependency installation and Flight endpoint reachability.

`auto` doesn't fall back after a row batch, for an execution timeout, or for a schema, authentication, permission, conversion, or stream failure.

## Does batch_size limit Flight memory?

Not completely. The connector slices Flight output into Arrow tables with at most `batch_size` rows. Doris or the Arrow Database Connectivity (ADBC) driver can produce or prefetch a larger RecordBatch before that slice occurs.

## Does a datasource instance refresh table metadata?

No. The instance caches its first schema and tablet discovery result because Ray can call `get_read_tasks()` more than once for one Dataset construction. Create a new datasource instance for a later logical read.

## Does the planning cache provide snapshot isolation?

No. Planning and split execution occur through separate Doris requests. Concurrent writes can affect tasks differently.

## Can a task resume a partial split?

No. Ray can retry the complete read task according to its remote arguments. `ray-doris` doesn't track a row offset or resume token inside a split.

## What account privileges does ray-doris require?

The account needs `SELECT_PRIV` on the target internal-catalog table and network access to the frontend MySQL and HTTP ports. Add Flight endpoint access for Flight reads. The connector doesn't require administrator-only tablet metadata commands.

## Does ray-doris serialize my password?

A literal `password` remains in serialized task state for compatibility. With `password_env`, only
the variable name is serialized; the driver and each worker resolve its value immediately before a
network connection. Inject the same variable into every eligible Ray process.

## Does ray-doris manage Doris FE failover?

No. Configure one logical FE hostname. The connector verifies TLS and reopens connections for new
planning and task attempts, but Doris and your external load balancer own FE discovery, leader
election, quorum, health checks, and backend failover.

## Where do I install the Flight extra?

Install the extra on the driver and in every environment that can execute a Ray read task. The driver validates explicit `flight` requests, and worker-side dependency availability determines whether `auto` can use Flight.

---
myst:
  html_meta:
    description: "Experimental shared-storage reads using Doris metadata, Hive Metastore storage descriptors, and Ray native Parquet readers."
---

# Automatically read shared Hive Parquet files

The experimental `auto_native_read=True` identifies an ordinary Hive table through
Doris and selects Ray's public Parquet reader when its metadata and storage fit
the supported profile. The flag defaults to `False`: reads go through Doris and
do not probe HMS or native storage. Supply `catalog` for an external table.
The API accepts a table identifier; complete SQL statements and SQL rewriting are
unsupported. There is no `read_mode` parameter or force-native public mode.

From a source checkout containing this experimental feature, install the optional
metadata SDK. This development work has not yet been
published as a package release:

```bash
pip install -e ".[native]"
```

```python
from ray_doris import NativeReadOptions, read_doris

dataset = read_doris(
    host="doris-fe.example.com",
    catalog="hive_catalog",
    table="analytics.published_events",
    auto_native_read=True,
    user="metadata_reader",
    password_env="DORIS_PASSWORD",
    native_options=NativeReadOptions(max_partitions=10_000, max_files=100_000),
    columns=["event_id", "region"],
    concurrency=4,
)
```

## Understand source selection

The driver makes one source decision before returning the Dataset:

| Condition | Selected path |
| --- | --- |
| The flag is disabled | Read through Doris without native discovery |
| The Hive table, session, files and public Ray capabilities fit the native profile | Use public Ray Parquet reading |
| A known valid context is outside that profile | Keep Doris reading and log the reason |
| Authentication, connection, source access, missing objects or contradictory metadata fail | Raise the error |
| An error occurs after native execution starts | Raise the error without replaying through Doris |

Only a specific not-applicable decision permits planning fallback. Ordinary
planning errors are not treated as optimization misses. External tables use one
fully qualified table query through the existing bounded MySQL/Flight readers;
they do not use the internal tablet query-plan endpoint. Internal reads retain
their existing planning behavior. Ray output blocks do not multiply that external query.

## Review the experimental boundary

Eligibility is determined by metadata and reader capabilities, without an exact
Doris or Ray patch-version whitelist. The package's Ray installation window
remains `>=2.49.2,<2.60`. The real Doris integration targets are 4.0.6 and
3.0.6.2 with Python 3.12 and Ray 2.59.0. These are test baselines, not minimum
Doris releases. A different Doris release can qualify when the same contracts
are available; this does not certify every deployment or authentication provider.
See `tests/it-ledger.md` for actual validation results.
Real local-file reader contracts also cover Ray 2.49.2 V1 and Ray 2.58.0 V1/V2;
these checks do not extend the HDFS/S3 cluster matrix to those Ray releases.

- Native selection requires Hive Parquet without transactions, with standard
  InputFormat and SerDe on HDFS or S3. Other formats, Hive ACID, views,
  Iceberg/Hudi/Delta and custom handlers in Hive retain Doris reading where supported by Doris.
- Publish a stable batch before reading. Files, schema and registered partitions
  must remain unchanged throughout consumption. A file list is not a snapshot.
- Data columns use the existing Doris-to-Arrow mapping and must agree with HMS.
  The initial Hive types are BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT,
  DOUBLE, STRING, DECIMAL, DATE and TIMESTAMP. CHAR/VARCHAR padding semantics,
  general schema evolution, positional mapping and nested types are outside the
  native profile. Before selecting native reading, every candidate file's
  Parquet footer is checked for exact data-column names and compatible
  types before reading. Case-only field differences and missing/extra fields
  fail visibly instead of being filled with NULL. Partition columns must be
  absent from the physical files, as in Spark's standard partitioned output.
- Partition keys initially support STRING and BIGINT. Only registered standard
  `key=value` locations under the table root are accepted. Custom locations,
  NULL/default partitions, mixed storage and visible nested data directories
  retain Doris reading. Native enumeration excludes unregistered partitions
  and hidden control files.
- Missing registered locations fail visibly, including when Doris itself would
  ignore absent partitions. Reaching a native enumeration limit retains Doris
  reading without truncating the table.
  An Arrow directory listing may still allocate its complete result before the
  file-count check; these limits are not a hard driver memory bound.

Native reading maps Parquet data columns by name. The driver checks the effective
`hive_parquet_use_column_names` setting; `false` retains Doris execution. For
reference SQL queries, use `SET hive_parquet_use_column_names = true;` in the
Doris SQL session. The connector does not change session settings automatically.

## Try Iceberg REST catalog reads

The `ray-doris[iceberg]` extra provides a separate experimental profile for a
Doris Iceberg Catalog backed by an independent Iceberg REST Catalog. It uses the
same `catalog`, `table`, and `auto_native_read=True` arguments; no snapshot
argument is needed. The real-service checks and remaining limits are listed below.
The Hive options and the Hive catalog path above do not configure Iceberg FileIO.

The driver checks Doris catalog properties, the caller's visible Doris policies,
the actual requested columns and an independently configured REST identity. When
available, shared immutable metadata and the same table UUID establish identity.
On older versions such as Doris 3.0.6.2, the connector instead compares the selected
snapshot ID and manifest list, or those of a reachable common ancestor. This is
weaker evidence: it does not prove the current Doris table UUID, and the connector
logs a warning when it selects this path. Missing evidence retains Doris execution;
conflicting manifest metadata is an error.

Before enabling this profile, ensure the Doris and Ray REST identities resolve the
same table and that the actual table and storage access are authorized. The
connector cannot independently verify hidden identity-based routing or every table
replacement; enabling the option is not proof of authorization equivalence.
On Doris 3.0.6.2, `iceberg_meta` also checks table-level `SELECT` privilege; its
permission errors are reported even if a projected ordinary query is permitted.
Custom
authorization controllers, unavailable policy information, remote name mapping,
explicit concurrency and non-default `NativeReadOptions` also retain Doris.

For eligible reads, Doris's `main` ref supplies the snapshot when available;
otherwise the REST current snapshot is used only after the required identity
checks. A selected snapshot is passed to Ray and to any Doris fallback. Ray's
deferred catalog load rechecks the REST table UUID and snapshot. A later ordinary
Doris query can see a different snapshot because of concurrent writes or caches;
on the ancestor path Doris may not yet be able to read the selected REST snapshot.
If native planning is known to be incompatible after snapshot selection, Doris
fallback must read that same snapshot or fail. An expired snapshot, inconsistent
metadata or a failed native execution is an error; results from native and Doris
execution are never combined.

PyIceberg uses its existing catalog configuration and environment for REST
authentication and storage access. Its S3 credential delegation does not imply
Doris policy equivalence, a particular credential lifetime, or HDFS authorization.
When absent from the named PyIceberg configuration, the connector can reuse
visible Doris S3 endpoint and region, and `OAuth2` server `URI` and scope as
non-secret defaults. Explicit PyIceberg settings and REST server overrides keep
their existing precedence. The connector never forwards Doris credentials;
S3 path-style options still require independent PyIceberg configuration.
Automatically reused endpoints must be HTTP(S) URLs without embedded credentials,
query strings or fragments; configure other supported forms explicitly.
Configure the catalog before PyIceberg loads in each relevant process: changing
environment variables later does not necessarily refresh its cached configuration.
HDFS uses the deployed Hadoop identity and configuration on the driver and Ray
workers. With PyIceberg 0.10's PyArrow FileIO, qualified HDFS paths do not pass
the `hdfs.user` or `hdfs.kerberos_ticket` properties to the `URI` constructor.
Kerberos credentials and renewal require separate deployment validation.
Never copy masked Doris secrets into Ray; use a trusted Ray cluster
because Iceberg FileIO and credentials may be serialized to workers. The connector
currently leaves the REST service's management origin unknown; it does not query
a Gravitino management server or infer management from the URL or credential
delegation. Gravitino-governed policy equivalence has not been verified.

Known unsupported delete representations and file formats retain a snapshot-pinned
Doris query when possible; this feature does not write or delete Iceberg data.
Real-service checks cover S3 and HDFS on Doris `4.0.6-rc02`, S3 on
`4.1.5-release`, and S3 snapshot and cached-ancestor native reads on
`3.0.6.2-rc01`. The 3.0.6.2 native path has not been verified with HDFS or
schema evolution.
A 4.0.6 check also
covered static S3 credential vending, table replacement, snapshot expiration
and a failed native read. Targeted checks cover worker retry at a pinned snapshot,
row-policy fallback, an empty table, a simulated unsupported delete falling back
to Doris at the same snapshot, and an explicit error on incompatible historical
schema. A 4.1.5 Position Delete check compared automatic and Doris query results;
it did not separately assert the selected path. These checks do not establish
equality-delete or deletion-vector coverage, STS credential refresh, or Kerberos
support.

Native timestamps require an effective UTC Doris session; other session time zones
retain Doris execution. Adjusted timestamps use timezone-free UTC values,
including the validated UTC INT96 fixture. The connector checks the session but
does not apply arbitrary timezone transformations in Ray. Other writer/timezone
combinations remain unverified; this is not a promise of general timezone parity.

## Configure independent source access

Doris supplies catalog properties, logical columns, and registered partition metadata.
HMS supplies the original partition identifiers, storage descriptors and actual
locations. Native discovery currently requires plain Thrift HMS in a trusted network;
SASL/Kerberos and catalog name remapping retain Doris execution. An explicit `hms_uri`
must match a URI advertised by the Doris catalog.

### Discover metadata capabilities

The driver reuses Doris's structured metadata, in this priority order:

| Required fact | Preferred source | Alternate source |
| --- | --- | --- |
| Catalog identity and properties | `catalogs()` | `SHOW CATALOG` plus `SHOW CATALOGS` |
| Column names, complete types and nullability | External `information_schema.columns` | Fully qualified `DESCRIBE` |
| Registered partitions, for partitioned tables only | Typed `$partitions` values | `SHOW PARTITIONS` names compared with original HMS identities |
| Effective Parquet mapping and timestamp timezone | Session `@@` variables | `SHOW VARIABLES LIKE ...` |

The external Doris planner shares the same column discovery and type parser with
the native path. External `DESCRIBE` display aliases such as `TEXT` are normalized
locally; logical types must still agree with HMS. The driver never infers raw
partition paths from typed values: names such as `q=01` and `q=1` remain distinct.
Only timestamp tables require the timezone setting. The Doris build string is
diagnostic and does not select a metadata strategy.

A recognized absent interface permits the next provider. Empty relational catalog
or column metadata triggers a native visibility check. Permission, connection,
malformed-result and consistency errors remain errors. If no compatible catalog,
partition or required setting interface exists, automatic selection retains Doris
execution. Doris execution still needs a usable column description. Discovery
results and their origins belong to one driver read, without a global metadata cache.

Storage access is independent of Doris permissions. Supply an Arrow `filesystem`
in `NativeReadOptions`, or configure Arrow's standard environment and credential
providers on the driver and every eligible Ray worker. HDFS requires compatible
Java, Hadoop configuration and libhdfs on every node. S3 endpoints and credentials
must address the same store as the catalog. Secrets are not recovered from Doris
masked attributes. Arrow filesystem serialization may carry authentication state;
use a trusted Ray cluster and read-only storage identities.

On Linux with HotSpot, Ray and the embedded HDFS JVM can conflict over signal
handlers. Load the matching JDK's `libjsig.so` through `LD_PRELOAD` before
starting Python on every HDFS-capable driver, head and worker. Setting it inside
an already running Python process is too late. The integration runtime uses this
signal-chaining setup and keeps Ray's native failure diagnostics enabled. See
[Ray's HDFS/JVM guidance](https://docs.ray.io/en/master/ray-observability/user-guides/debug-apps/debug-failures.html#jvm-crashes-when-using-pyarrow-with-hdfs).

Doris must remain available for SQL metadata queries. Some metadata functions
also require a healthy Doris backend; native reading bypasses business-data scans,
not the metadata query execution requirements.

Native reads do not execute Doris row filters or column masking. Enable automatic
native selection only where direct source access is independently authorized for the complete selected
data. Catalogs declaring a custom access controller retain Doris execution.
The connector does not enumerate or reproduce all global authorization policies;
deployments that require Doris-side row filtering or masking must keep the flag
disabled. Default and fallback table reads execute through Doris.

## Use Ray execution controls

`columns`, `concurrency`, `override_num_blocks` and `ray_remote_args` are delegated
to the public reader. A SQL-string `filter`, transport/tablet/batch options that
differ from defaults, or client settings outside the native allowlist retain Doris execution.
Tablet and HTTP planning settings apply only to internal tables. Apply public
Ray Dataset expression APIs to the returned Dataset when native filtering is desired.
`native_options` requires the automatic switch to be enabled.
Ray performs file decoding and partition synthesis. A public `map_batches`
operation then restores the declared column order, nullability and safe Arrow
types. Footer checks add one metadata read per file; they do not provide a snapshot.
An empty file list or files whose footer row counts are all zero return an empty
Dataset with the declared schema, including projected partition columns.

The connector checks named public parameters on `read_parquet`, `map_batches`
and, for partitioned tables, `Partitioning`. It passes optional reader arguments
only when they are explicitly supported; legacy readers do not receive the newer
`ignore_missing_paths` argument. A public `use_datasource_v2` setting identifies
the active reader. The legacy public signature without that setting and without
`ignore_missing_paths` identifies V1; an unknown combination retains Doris reading.
No reader implementation is selected from a Doris or Ray version string.
Legacy readers receive an explicit empty partition scheme for tables without
partition keys, preserving column projection without inferring keys from parent directories.

The configured V2 reader replaces `file_extensions=None` with its default
Parquet suffix filter. Visible input files without the `.parquet` suffix therefore
retain Doris reading, preventing silent omission in mixed lists. A caller can
explicitly select Ray's public V1 setting to make validated Parquet files without a suffix
eligible before creating the Dataset:

```python
import ray.data

ray.data.DataContext.get_current().use_datasource_v2 = False
```

V2 also reserves the physical data-column name `path`. Nonempty tables
containing that data column retain Doris reading under V2; the same public V1
setting permits native reading. A path-derived partition key named `path` is
not a physical data column and does not have this restriction.

The connector does not change that global setting. Native reader failures remain
visible; retries, task scheduling, output blocks and cancellation belong to Ray.
Performance gains depend on the workload and are not guaranteed.

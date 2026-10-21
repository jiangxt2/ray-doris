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
metadata SDK and the initial Ray baseline. This development work has not yet been
published as a package release:

```bash
pip install -e ".[native]" "ray[data]==2.59.0"
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
| The Hive table, session, files and Ray version fit the native profile | Use public Ray Parquet reading |
| A known valid context is outside that profile | Keep Doris reading and log the reason |
| Authentication, connection, source access, missing objects or contradictory metadata fail | Raise the error |
| An error occurs after native execution starts | Raise the error without replaying through Doris |

Only a specific not-applicable decision permits planning fallback. Ordinary
planning errors are not treated as optimization misses. External tables use one
fully qualified table query through the existing bounded MySQL/Flight readers;
they do not use the internal tablet query-plan endpoint. Internal reads retain
their existing planning behavior. Ray output blocks do not multiply that external query.

## Review the experimental boundary

The initial validation targets are Doris 4.0.6 and 3.0.6.2 with Python 3.12 and
Ray 2.59.0. See the integration ledger for actual results; listing a target does
not certify every deployment or authentication provider.

- Native selection requires Hive Parquet without transactions, with standard
  InputFormat and SerDe on HDFS or S3. Other formats, Hive ACID, views,
  Iceberg/Hudi/Delta and custom handlers retain Doris reading where supported by Doris.
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

Native timestamps require an effective UTC Doris session; other session time zones
retain Doris execution. Adjusted timestamps use timezone-free UTC values,
including the validated UTC INT96 fixture. The connector checks the session but
does not apply arbitrary timezone transformations in Ray. Other writer/timezone
combinations remain unverified; this is not a promise of general timezone parity.

## Configure independent source access

Doris supplies catalog properties, logical columns, and typed partition values.
HMS supplies the original partition identifiers, storage descriptors and actual
locations. Native discovery currently requires plain Thrift HMS in a trusted network;
SASL/Kerberos and catalog name remapping retain Doris execution. An explicit `hms_uri`
must match a URI advertised by the Doris catalog.

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

Ray 2.59's default V2 reader replaces `file_extensions=None` with its default
Parquet suffix filter. Visible input files without the `.parquet` suffix therefore
retain Doris reading, preventing silent omission in mixed lists. A caller can
explicitly select Ray's public V1 setting to make validated Parquet files without a suffix
eligible before creating the Dataset:

```python
import ray.data

ray.data.DataContext.get_current().use_datasource_v2 = False
```

Ray 2.59 V2 also reserves the physical data-column name `path`. Nonempty tables
containing that data column retain Doris reading under V2; the same public V1
setting permits native reading. A path-derived partition key named `path` is
not a physical data column and does not have this restriction.

The connector does not change that global setting. Native reader failures remain
visible; retries, task scheduling, output blocks and cancellation belong to Ray.
Performance gains depend on the workload and are not guaranteed.

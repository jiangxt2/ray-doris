---
myst:
  html_meta:
    description: "Experimental shared-storage reads using Doris metadata, Hive Metastore storage descriptors, and Ray native Parquet readers."
---

# Read shared Hive Parquet files

The experimental `read_mode="native"` identifies an ordinary Hive table through
Doris, discovers its storage metadata, and returns a Dataset from Ray's public
Parquet reader. The default `read_mode="doris"` reads internal Doris tables.
Both modes accept a table identifier; complete SQL statements and automatic SQL
rewriting are unsupported. Native reads never fall back to a Doris data query.

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
    read_mode="native",
    user="metadata_reader",
    password_env="DORIS_PASSWORD",
    native_options=NativeReadOptions(max_partitions=10_000, max_files=100_000),
    columns=["event_id", "region"],
    concurrency=4,
)
```

## Review the experimental boundary

The initial validation targets are Doris 4.0.6 and 3.0.6.2 with Python 3.12 and
Ray 2.59.0. See the integration ledger for actual results; listing a target does
not certify every deployment or authentication provider.

- Use Hive Parquet without transactions, with standard InputFormat and SerDe on HDFS
  or S3. Hive ACID, views, Iceberg, Hudi, Delta and custom handlers are rejected.
- Publish a stable batch before reading. Files, schema and registered partitions
  must remain unchanged throughout consumption. A file list is not a snapshot.
- Data columns use the existing Doris-to-Arrow mapping and must agree with HMS.
  The initial Hive types are BOOLEAN, TINYINT, SMALLINT, INT, BIGINT, FLOAT,
  DOUBLE, STRING, DECIMAL, DATE and TIMESTAMP. CHAR/VARCHAR padding semantics,
  general schema evolution, positional mapping and nested types are unsupported.
  Every Parquet footer is checked for exact data-column names and compatible
  types before reading. Case-only field differences and missing/extra fields
  fail visibly instead of being filled with NULL. Partition columns must be
  absent from the physical files, as in Spark's standard partitioned output.
- Partition keys initially support STRING and BIGINT. Only registered standard
  `key=value` locations under the table root are accepted. Custom locations,
  NULL/default partitions, mixed storage and visible nested data directories
  are rejected. Unregistered partitions and hidden control files are excluded.
- Missing registered locations fail visibly, including when Doris itself would
  ignore absent partitions. Enumeration limits reject the read without truncation.
  An Arrow directory listing may still allocate its complete result before the
  file-count check; these limits are not a hard driver memory bound.

Native mode maps Parquet data columns by name. For comparisons with Doris SQL,
use `SET hive_parquet_use_column_names = true;` in that SQL session. This variable
controls Doris's reader; native mode neither reads nor changes it. The positional
mapping selected by `false` remains outside this profile.

The initial timestamp contract uses UTC: adjusted timestamps are returned as
timezone-free UTC values, including the validated UTC INT96 fixture. Native mode
does not apply a Doris SQL session's display timezone. Comparing timestamp values
with Doris SQL therefore requires a UTC Doris session. Other writer/timezone
combinations remain unverified; this is not a promise of general timezone parity.

## Configure independent source access

Doris supplies catalog properties, logical columns, and typed partition values.
HMS supplies the original partition identifiers, storage descriptors and actual
locations. Native mode currently requires plain Thrift HMS in a trusted network;
SASL/Kerberos and catalog name remapping are unsupported. An explicit `hms_uri`
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
also require a healthy Doris backend; native mode bypasses business-data scans,
not the metadata query execution requirements.

Native reads do not execute Doris row filters or column masking. Use them only
where direct source access is independently authorized for the complete selected
data. Use an appropriate Doris SQL client when required policies depend on Doris
execution. This connector's default `doris` mode currently targets internal tables;
it does not provide a general external-table SQL fallback.

## Use Ray execution controls

`columns`, `concurrency`, `override_num_blocks` and `ray_remote_args` are delegated
to the public reader. Query-only transport, tablet, HTTP planning, Flight and
custom `batch_size` options are rejected. SQL-string `filter` is rejected;
apply the supported public Ray Dataset expression API to the returned Dataset.
Ray performs file decoding and partition synthesis. A public `map_batches`
operation then restores the declared column order, nullability and safe Arrow
types. Footer checks add one metadata read per file; they do not provide a snapshot.
An empty file list or files whose footer row counts are all zero return an empty
Dataset with the declared schema, including projected partition columns.

Ray 2.59's default V2 reader replaces `file_extensions=None` with its default
Parquet suffix filter. The connector therefore rejects any visible input file
without the `.parquet` suffix under V2, preventing silent omission in mixed lists.
For validated files without a suffix, explicitly select Ray's public V1 setting
before creating the Dataset:

```python
import ray.data

ray.data.DataContext.get_current().use_datasource_v2 = False
```

Ray 2.59 V2 also reserves the physical data-column name `path`. Nonempty tables
containing that data column are explicitly rejected under V2; select the same
public V1 setting to preserve it. A path-derived partition key named `path` is
not a physical data column and does not have this restriction.

The connector does not change that global setting. Native reader failures remain
visible; retries, task scheduling, output blocks and cancellation belong to Ray.
Performance gains depend on the workload and are not guaranteed.

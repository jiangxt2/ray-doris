# Native shared-storage integration

The runner reuses the cached Doris 4.0.6 and 3.0.6.2 FE/BE images, Hive 3.1.3 /
Hadoop 3.3.0 image, Spark 3.5.8, MinIO, and Ray 2.59.0 / Python 3.12. The initial
local image set is Linux ARM64; the test runtime copies that image's ARM64 JDK.
It is not a declaration of an additional CPU-architecture matrix.

From the approved feature worktree:

```bash
.venv/bin/python tests/native_integration/prepare_libhdfs.py \
  --destination /tmp/rd-hadoop-jni-source
.venv/bin/python tests/native_integration/run.py \
  --hadoop-source /tmp/rd-hadoop-jni-source --logs /tmp/rd-native-unique-run
```

The cached Hive image contains x86-64 Hadoop native binaries despite its ARM64
image metadata. Configure standard HTTP(S) proxy variables for GitHub access
(the local workspace uses PandaFan). Preparation fetches the official Hadoop 3.3.0 JNI sources at
`aa96f1871bfd858f9bac59cf2a81ec470da649af`, verifies each Git blob,
and records a source manifest. The runtime compiles the six upstream libhdfs
translation units for ARM64 and verifies that the resulting library loads. It
does not replace the connector's file reader or require building the Hadoop JVM
services. Reuse this immutable source directory in later repairs.

The log directory must not exist. The runner refuses pre-existing `ray-doris-it`
resources, prebuilds each distinct custom image once, and uses Compose `--no-build`.
It records the cumulative source/config fingerprint and complete base image IDs,
retains command output and XML, and removes only its Compose resources and new
custom tags on exit. It never prunes bases, unrelated resources or build cache.

Spark writes stable Parquet and ORC files into HDFS. The source fixture copies those exact
Spark-produced bytes to MinIO and registers independent HDFS and S3 Hive tables.
This verifies both storage readers; it does not certify Spark's S3A writer setup.
Real Doris external-table SQL is the independent data reference. A decoy unregistered
partition verifies that root-directory discovery cannot add table contents.

The matrix tests structured metadata, typed registered partitions, scalar schema,
projection, empty tables, enumeration limits, permissions and execution across two
Ray workers. Automatic selection tests compare actual Doris fallback results for
ORC, unsupported native layouts, enumeration budgets, session settings and Ray V2
limitations; the disabled switch must avoid native discovery. Real source errors
and native runtime failures must not replay through Doris. The 4.0.6 stage also executes the existing
query/write integration suite from the host runtime. The head reserves no CPUs.
Native reads are hard-pinned to each worker in turn for both HDFS and S3; the
test checks that the real public reader receives that constraint and records
row counts and node identities. Metadata cases include `get_schema_from_table`,
distinct raw partitions `01`/`1`, empty registered partitions and missing
registered locations. Cleanup handles SIGTERM, reaps active CLI children and
verifies the absence of owned resources before reporting success.

When an unchanged query/write regression result is explicitly reused, pass
`--skip-query-regression` and record the reason and prior evidence in the ledger.
This does not skip either selected version's native suite.

Only rerun failed, interrupted or affected states. A passed source/config/environment
state must reuse its existing result. Store results in `tests/it-ledger.md`; a passing
native matrix is functional evidence, not a performance or release certification.

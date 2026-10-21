"""Publish immutable Spark-produced Parquet to the test HDFS service."""

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("ray-doris-native-fixture")
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    .config("spark.hadoop.fs.defaultFS", "hdfs://hms:9000")
    .getOrCreate()
)
data = spark.range(240).selectExpr(
    "cast(id as int) id",
    "concat('row-', id) payload",
    "cast(id / 10 as decimal(12,2)) amount",
    "DATE '2026-10-20' day",
    "TIMESTAMP '2026-10-20 12:34:56.123456' ts",
    "id % 2 = 0 flag",
    "cast(id % 100 as tinyint) tiny",
    "cast(id as smallint) small",
    "cast(id + 8589934592 as bigint) large",
    "cast(id % 3 as float) real",
    "cast((id % 7) / 8 as double) score",
    "case when id % 5 = 0 then NULL else 'ok' end maybe",
    "case when id % 3 = 0 then 'east' when id % 3 = 1 then 'west' else 'north/south' end p",
    "cast(floor(id / 3) % 3 as bigint) q",
)
data.drop("p", "q").repartition(4).write.mode("error").parquet("/native/plain")
data.repartition(4).write.mode("error").partitionBy("p", "q").parquet("/native/partitioned")
spark.conf.set("spark.sql.parquet.outputTimestampType", "INT96")
data.drop("p", "q").repartition(4).write.mode("error").parquet("/native/int96")
spark.stop()

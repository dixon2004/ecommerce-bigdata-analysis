# Databricks notebook source
# MAGIC %md
# MAGIC # Step 0 (Databricks): Ingest the raw CSV(.gz) once and convert it to Parquet
# MAGIC
# MAGIC Same conversion as `scripts/local/00_prepare_data.py` (see that file for
# MAGIC the full "why Parquet first" rationale), adapted for a Databricks notebook:
# MAGIC SparkSession is provided automatically as `spark`, and paths come from
# MAGIC widgets instead of CLI args.
# MAGIC
# MAGIC **Before running:** upload `2020-Apr.csv.gz` to a Databricks Volume or DBFS
# MAGIC path and set `input_path` below. Import this file into your Databricks
# MAGIC workspace as a notebook (File > Import), attach it to a cluster, and Run
# MAGIC All once (before running 01/02 in this same folder) to produce the Parquet
# MAGIC dataset.
# MAGIC
# MAGIC The explicit schema is much faster than `inferSchema=True` on a ~50M row
# MAGIC file, and guarantees consistent types across every environment and run.

# COMMAND ----------

dbutils.widgets.text(
    "input_path", "/Volumes/workspace/default/big-data/2020-Apr.csv.gz", "Input CSV path"
)
dbutils.widgets.text(
    "output_path", "/Volumes/workspace/default/big-data/parquet", "Output Parquet path"
)
dbutils.widgets.text("partitions", "16", "Output partitions")

INPUT_PATH = dbutils.widgets.get("input_path")
OUTPUT_PATH = dbutils.widgets.get("output_path")
NUM_PARTITIONS = int(dbutils.widgets.get("partitions"))

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

SCHEMA = StructType(
    [
        StructField("event_time", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("category_id", LongType(), True),
        StructField("category_code", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("user_id", LongType(), True),
        StructField("user_session", StringType(), True),
    ]
)

# Raw timestamps look like "2020-04-01 00:00:00 UTC"
EVENT_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss 'UTC'"

# COMMAND ----------

df = (
    spark.read.option("header", True)
    .option("compression", "gzip")
    .schema(SCHEMA)
    .csv(INPUT_PATH)
)

df = df.withColumn(
    "event_time", to_timestamp(col("event_time"), EVENT_TIME_PATTERN)
).repartition(NUM_PARTITIONS)

# Serverless compute doesn't support cache()/persist(), so write first and
# count the Parquet output (row counts come straight from the footers).
df.write.mode("overwrite").parquet(OUTPUT_PATH)
row_count = spark.read.parquet(OUTPUT_PATH).count()

print(f"Rows written: {row_count}")
print(f"Parquet written to: {OUTPUT_PATH}")

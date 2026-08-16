"""
Step 0 - Ingest the raw CSV(.gz) once and convert it to Parquet.

Why this step exists
---------------------
The raw file (2020-Apr.csv.gz) is a single gzip-compressed CSV. Gzip is NOT a
splittable format, so no matter how many cores/executors are available,
Spark reads the whole file as exactly ONE task on the initial scan. That
makes every later benchmark pay a fixed, non-parallel decode cost before any
real distributed work happens - which would pollute the local-vs-Databricks
and DataFrame-vs-SQL comparisons with a bottleneck that has nothing to do
with the analysis itself.

Converting to Parquet once (columnar, splittable, compressed, schema-aware)
means all four benchmark scripts read from a fair, realistic starting point -
which is also standard big-data practice (raw ingest once, analytics reads
from an optimized format many times).

The explicit SCHEMA below is much faster than inferSchema=True on a ~50M row
file, and guarantees consistent types across every environment and run.

Usage
-----
    spark-submit scripts/local/00_prepare_data.py \
        --input  data/raw/2020-Apr.csv.gz \
        --output data/parquet

For Databricks, use scripts/databricks/00_prepare_data.py instead (same
conversion, adapted to run as a notebook with widgets for the paths).
"""
import argparse

from pyspark.sql import SparkSession
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


def main(input_path: str, output_path: str, num_partitions: int = 16) -> None:
    spark = SparkSession.builder.appName("prepare_ecommerce_data").getOrCreate()

    df = (
        spark.read.option("header", True)
        .option("compression", "gzip")
        .schema(SCHEMA)
        .csv(input_path)
    )

    df = df.withColumn(
        "event_time", to_timestamp(col("event_time"), EVENT_TIME_PATTERN)
    ).repartition(num_partitions)

    # Write first, then count the Parquet output rather than caching the
    # CSV-derived DataFrame: the CSV is still decoded only once, but counting
    # Parquet reads row counts straight from the footers, so the driver never
    # has to hold 50M+ rows in memory.
    df.write.mode("overwrite").parquet(output_path)
    row_count = spark.read.parquet(output_path).count()

    print(f"Rows written: {row_count}")
    print(f"Parquet written to: {output_path}")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Path to 2020-Apr.csv.gz")
    parser.add_argument("--output", required=True, help="Output Parquet directory")
    parser.add_argument(
        "--partitions",
        type=int,
        default=16,
        help="Number of output partitions (tune to your core count)",
    )
    args = parser.parse_args()
    main(args.input, args.output, args.partitions)

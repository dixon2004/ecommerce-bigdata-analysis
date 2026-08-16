"""
Implementation 1 of 4: Apache Spark DataFrame API - LOCAL

Runs the five proposed analyses over the eCommerce behaviour dataset using
the Spark DataFrame API (method chaining: .groupBy(), .filter(), .agg() ...)
on a local Spark master (local[*] - uses all cores on this machine).

Analyses:
    1. Customer Activity Distribution   (event_type counts)
    2. Top 10 Most Purchased Products   (product_id, purchase count)
    3. Most Popular Product Categories  (category_code counts)
    4. Most Popular Brands              (brand counts)
    5. Customer Purchase Trends         (daily purchase count, April 2020)

Each analysis is timed independently (wall-clock, forced to execute via
.toPandas()) so its cost can be compared against:
    - the same analysis via Spark SQL, local             (scripts/local/02_spark_sql.py)
    - the same analysis via DataFrame API, on Databricks (scripts/databricks/01_dataframe_api.py)
    - the same analysis via Spark SQL, on Databricks     (scripts/databricks/02_spark_sql.py)

Usage:
    spark-submit scripts/local/01_dataframe_api.py --input data/parquet
"""
import argparse
import os
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, date_format, desc

RESULTS_DIR = "results/local/dataframe_api"
TIMINGS_PATH = "results/local/dataframe_api_timings.csv"

# analysis key -> (DataFrame transformation, output CSV filename). Keys and
# filenames match scripts/local/02_spark_sql.py so results are directly
# comparable row for row.
ANALYSES = {
    "1_activity_distribution": (
        lambda df: (
            df.groupBy("event_type")
            .agg(count("*").alias("event_count"))
            .orderBy(desc("event_count"))
        ),
        "01_activity_distribution.csv",
    ),
    "2_top_10_purchased_products": (
        lambda df: (
            df.filter(col("event_type") == "purchase")
            .groupBy("product_id")
            .agg(count("*").alias("purchase_count"))
            .orderBy(desc("purchase_count"))
            .limit(10)
        ),
        "02_top_10_purchased_products.csv",
    ),
    "3_top_categories": (
        lambda df: (
            df.filter(col("category_code").isNotNull())
            .groupBy("category_code")
            .agg(count("*").alias("interaction_count"))
            .orderBy(desc("interaction_count"))
            .limit(15)
        ),
        "03_top_categories.csv",
    ),
    "4_top_brands": (
        lambda df: (
            df.filter(col("brand").isNotNull())
            .groupBy("brand")
            .agg(count("*").alias("interaction_count"))
            .orderBy(desc("interaction_count"))
            .limit(15)
        ),
        "04_top_brands.csv",
    ),
    "5_daily_purchase_trend": (
        lambda df: (
            df.filter(col("event_type") == "purchase")
            .withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))
            .groupBy("event_date")
            .agg(count("*").alias("purchase_count"))
            .orderBy("event_date")
        ),
        "05_daily_purchase_trend.csv",
    ),
}


def time_it(label, fn, timings):
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    timings.append({"analysis": label, "seconds": elapsed})
    print(f"[{label}] {elapsed:.3f}s")
    return result


def main(input_path: str):
    os.makedirs(RESULTS_DIR, exist_ok=True)

    spark = (
        SparkSession.builder.appName("local_dataframe_api")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(input_path)
    # Warm-up: forces the Parquet scan, file listing and JVM warm-up to happen
    # once, outside the timed analyses, so timings reflect each analysis rather
    # than Spark's one-off startup cost. Load-bearing - do not remove.
    df.count()

    timings = []
    for label, (transform, filename) in ANALYSES.items():
        result_pdf = time_it(label, lambda: transform(df).toPandas(), timings)
        result_pdf.to_csv(f"{RESULTS_DIR}/{filename}", index=False)

    pd.DataFrame(timings).to_csv(TIMINGS_PATH, index=False)
    print(f"\nTimings written to {TIMINGS_PATH}")
    print(f"Results written to {RESULTS_DIR}/")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input", required=True, help="Path to the Parquet dataset from step 00"
    )
    args = parser.parse_args()
    main(args.input)

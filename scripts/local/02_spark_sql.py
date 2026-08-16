"""
Implementation 2 of 4: Spark SQL - LOCAL

Same five analyses as scripts/local/01_dataframe_api.py, same input, same
local master, same timing methodology - but every analysis is expressed as a
SQL string executed via spark.sql() against a temp view, instead of DataFrame
method chaining. Keeping everything else identical isolates the one thing
we actually want to compare: DataFrame API vs Spark SQL syntax/performance.

Usage:
    spark-submit scripts/local/02_spark_sql.py --input data/parquet
"""
import argparse
import os
import time

import pandas as pd
from pyspark.sql import SparkSession

RESULTS_DIR = "results/local/spark_sql"
TIMINGS_PATH = "results/local/spark_sql_timings.csv"

VIEW_NAME = "events"

# analysis key -> (SQL query, output CSV filename). Keys and filenames match
# scripts/local/01_dataframe_api.py so results are directly comparable row for row.
ANALYSES = {
    "1_activity_distribution": (
        f"""
        SELECT event_type, COUNT(*) AS event_count
        FROM {VIEW_NAME}
        GROUP BY event_type
        ORDER BY event_count DESC
        """,
        "01_activity_distribution.csv",
    ),
    "2_top_10_purchased_products": (
        f"""
        SELECT product_id, COUNT(*) AS purchase_count
        FROM {VIEW_NAME}
        WHERE event_type = 'purchase'
        GROUP BY product_id
        ORDER BY purchase_count DESC
        LIMIT 10
        """,
        "02_top_10_purchased_products.csv",
    ),
    "3_top_categories": (
        f"""
        SELECT category_code, COUNT(*) AS interaction_count
        FROM {VIEW_NAME}
        WHERE category_code IS NOT NULL
        GROUP BY category_code
        ORDER BY interaction_count DESC
        LIMIT 15
        """,
        "03_top_categories.csv",
    ),
    "4_top_brands": (
        f"""
        SELECT brand, COUNT(*) AS interaction_count
        FROM {VIEW_NAME}
        WHERE brand IS NOT NULL
        GROUP BY brand
        ORDER BY interaction_count DESC
        LIMIT 15
        """,
        "04_top_brands.csv",
    ),
    "5_daily_purchase_trend": (
        f"""
        SELECT date_format(event_time, 'yyyy-MM-dd') AS event_date,
               COUNT(*) AS purchase_count
        FROM {VIEW_NAME}
        WHERE event_type = 'purchase'
        GROUP BY date_format(event_time, 'yyyy-MM-dd')
        ORDER BY event_date
        """,
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
        SparkSession.builder.appName("local_spark_sql")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(input_path)
    df.createOrReplaceTempView(VIEW_NAME)
    # Warm-up, excluded from timings - see 01_dataframe_api.py. Load-bearing.
    df.count()

    timings = []
    for label, (query, filename) in ANALYSES.items():
        result_pdf = time_it(label, lambda: spark.sql(query).toPandas(), timings)
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

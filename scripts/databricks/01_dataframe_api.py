# Databricks notebook source
# MAGIC %md
# MAGIC # Implementation 3 of 4: Apache Spark DataFrame API - DATABRICKS
# MAGIC
# MAGIC Same five analyses and same timing methodology as
# MAGIC `scripts/local/01_dataframe_api.py`, but running on a Databricks cluster
# MAGIC (SparkSession is provided automatically as `spark` - no need to build one).
# MAGIC
# MAGIC **Before running:** run `scripts/databricks/00_prepare_data.py` first to
# MAGIC produce the Parquet folder, then set the `input_path` widget below to that
# MAGIC location. Import this file into your Databricks workspace as a notebook
# MAGIC (File > Import), attach it to a cluster, and Run All.

# COMMAND ----------

dbutils.widgets.text("input_path", "/Volumes/workspace/default/big-data/parquet", "Input Parquet path")
INPUT_PATH = dbutils.widgets.get("input_path")

RESULTS_ROOT = "/Volumes/workspace/default/big-data/results/databricks"
RESULTS_DIR = f"{RESULTS_ROOT}/dataframe_api"
TIMINGS_PATH = f"{RESULTS_ROOT}/dataframe_api_timings.csv"

dbutils.fs.mkdirs(RESULTS_DIR)

# COMMAND ----------

import time

import pandas as pd
from pyspark.sql.functions import col, count, date_format, desc

df = spark.read.parquet(INPUT_PATH)
# Warm-up, excluded from timings - see scripts/local/01_dataframe_api.py.
# Load-bearing: keeps Spark's one-off startup cost out of the numbers below.
df.count()

timings = []


def time_it(label, fn):
    start = time.perf_counter()
    result = fn()
    elapsed = time.perf_counter() - start
    timings.append({"analysis": label, "seconds": elapsed})
    print(f"[{label}] {elapsed:.3f}s")
    return result


# COMMAND ----------

# MAGIC %md ### 1. Customer Activity Distribution

# COMMAND ----------

def analysis_1():
    return (
        df.groupBy("event_type")
        .agg(count("*").alias("event_count"))
        .orderBy(desc("event_count"))
        .toPandas()
    )


activity_dist = time_it("1_activity_distribution", analysis_1)
display(activity_dist)

# COMMAND ----------

# MAGIC %md ### 2. Top 10 Most Purchased Products

# COMMAND ----------

def analysis_2():
    return (
        df.filter(col("event_type") == "purchase")
        .groupBy("product_id")
        .agg(count("*").alias("purchase_count"))
        .orderBy(desc("purchase_count"))
        .limit(10)
        .toPandas()
    )


top_products = time_it("2_top_10_purchased_products", analysis_2)
display(top_products)

# COMMAND ----------

# MAGIC %md ### 3. Most Popular Product Categories

# COMMAND ----------

def analysis_3():
    return (
        df.filter(col("category_code").isNotNull())
        .groupBy("category_code")
        .agg(count("*").alias("interaction_count"))
        .orderBy(desc("interaction_count"))
        .limit(15)
        .toPandas()
    )


top_categories = time_it("3_top_categories", analysis_3)
display(top_categories)

# COMMAND ----------

# MAGIC %md ### 4. Most Popular Brands

# COMMAND ----------

def analysis_4():
    return (
        df.filter(col("brand").isNotNull())
        .groupBy("brand")
        .agg(count("*").alias("interaction_count"))
        .orderBy(desc("interaction_count"))
        .limit(15)
        .toPandas()
    )


top_brands = time_it("4_top_brands", analysis_4)
display(top_brands)

# COMMAND ----------

# MAGIC %md ### 5. Customer Purchase Trends (daily, April 2020)

# COMMAND ----------

def analysis_5():
    return (
        df.filter(col("event_type") == "purchase")
        .withColumn("event_date", date_format(col("event_time"), "yyyy-MM-dd"))
        .groupBy("event_date")
        .agg(count("*").alias("purchase_count"))
        .orderBy("event_date")
        .toPandas()
    )


purchase_trend = time_it("5_daily_purchase_trend", analysis_5)
display(purchase_trend)

# COMMAND ----------

# MAGIC %md ### Save results + timings
# MAGIC
# MAGIC Results are written back to the Volume/DBFS path so they can be
# MAGIC downloaded (Data > Volumes, or `dbutils.fs.cp` to a local path) and
# MAGIC combined with the other three implementations' timings in
# MAGIC `scripts/03_compare_results.py`.
# MAGIC
# MAGIC The timings CSV sits *alongside* the `dataframe_api/` folder rather than
# MAGIC inside it, so the downloaded `results/databricks/` tree matches
# MAGIC `results/local/` exactly and needs no renaming.
# MAGIC
# MAGIC Unity Catalog Volume paths (`/Volumes/...`) are FUSE-mounted, so pandas
# MAGIC writes to them directly. On a legacy `dbfs:/` path instead, prefix
# MAGIC `RESULTS_ROOT` with `/dbfs/` for the local-file API pandas needs.

# COMMAND ----------

results = {
    "01_activity_distribution.csv": activity_dist,
    "02_top_10_purchased_products.csv": top_products,
    "03_top_categories.csv": top_categories,
    "04_top_brands.csv": top_brands,
    "05_daily_purchase_trend.csv": purchase_trend,
}

for filename, pdf in results.items():
    pdf.to_csv(f"{RESULTS_DIR}/{filename}", index=False)

timings_df = pd.DataFrame(timings)
timings_df.to_csv(TIMINGS_PATH, index=False)

print(f"Results written under {RESULTS_DIR}, timings written to {TIMINGS_PATH}")
display(timings_df)

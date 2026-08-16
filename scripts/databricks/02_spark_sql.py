# Databricks notebook source
# MAGIC %md
# MAGIC # Implementation 4 of 4: Spark SQL - DATABRICKS
# MAGIC
# MAGIC Same five analyses as `scripts/databricks/01_dataframe_api.py`, same
# MAGIC cluster, same timing methodology - but every analysis is expressed as SQL
# MAGIC (`spark.sql()`) instead of DataFrame method chaining, mirroring
# MAGIC `scripts/local/02_spark_sql.py`.
# MAGIC
# MAGIC **Before running:** set `input_path` to the same Parquet location used in
# MAGIC `01_dataframe_api.py`, so results are directly comparable. Import into your
# MAGIC Databricks workspace as a notebook and Run All on the same cluster you
# MAGIC used for `01_dataframe_api.py` (keep cluster size identical between the two
# MAGIC runs so the DataFrame-vs-SQL comparison isn't confounded by different
# MAGIC hardware).

# COMMAND ----------

dbutils.widgets.text("input_path", "/Volumes/workspace/default/big-data/parquet", "Input Parquet path")
INPUT_PATH = dbutils.widgets.get("input_path")

RESULTS_ROOT = "/Volumes/workspace/default/big-data/results/databricks"
RESULTS_DIR = f"{RESULTS_ROOT}/spark_sql"
TIMINGS_PATH = f"{RESULTS_ROOT}/spark_sql_timings.csv"

dbutils.fs.mkdirs(RESULTS_DIR)

# COMMAND ----------

import time

import pandas as pd

VIEW_NAME = "events"

df = spark.read.parquet(INPUT_PATH)
df.createOrReplaceTempView(VIEW_NAME)
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

activity_dist = time_it(
    "1_activity_distribution",
    lambda: spark.sql(
        f"""
        SELECT event_type, COUNT(*) AS event_count
        FROM {VIEW_NAME}
        GROUP BY event_type
        ORDER BY event_count DESC
        """
    ).toPandas(),
)
display(activity_dist)

# COMMAND ----------

# MAGIC %md ### 2. Top 10 Most Purchased Products

# COMMAND ----------

top_products = time_it(
    "2_top_10_purchased_products",
    lambda: spark.sql(
        f"""
        SELECT product_id, COUNT(*) AS purchase_count
        FROM {VIEW_NAME}
        WHERE event_type = 'purchase'
        GROUP BY product_id
        ORDER BY purchase_count DESC
        LIMIT 10
        """
    ).toPandas(),
)
display(top_products)

# COMMAND ----------

# MAGIC %md ### 3. Most Popular Product Categories

# COMMAND ----------

top_categories = time_it(
    "3_top_categories",
    lambda: spark.sql(
        f"""
        SELECT category_code, COUNT(*) AS interaction_count
        FROM {VIEW_NAME}
        WHERE category_code IS NOT NULL
        GROUP BY category_code
        ORDER BY interaction_count DESC
        LIMIT 15
        """
    ).toPandas(),
)
display(top_categories)

# COMMAND ----------

# MAGIC %md ### 4. Most Popular Brands

# COMMAND ----------

top_brands = time_it(
    "4_top_brands",
    lambda: spark.sql(
        f"""
        SELECT brand, COUNT(*) AS interaction_count
        FROM {VIEW_NAME}
        WHERE brand IS NOT NULL
        GROUP BY brand
        ORDER BY interaction_count DESC
        LIMIT 15
        """
    ).toPandas(),
)
display(top_brands)

# COMMAND ----------

# MAGIC %md ### 5. Customer Purchase Trends (daily, April 2020)

# COMMAND ----------

purchase_trend = time_it(
    "5_daily_purchase_trend",
    lambda: spark.sql(
        f"""
        SELECT date_format(event_time, 'yyyy-MM-dd') AS event_date,
               COUNT(*) AS purchase_count
        FROM {VIEW_NAME}
        WHERE event_type = 'purchase'
        GROUP BY date_format(event_time, 'yyyy-MM-dd')
        ORDER BY event_date
        """
    ).toPandas(),
)
display(purchase_trend)

# COMMAND ----------

# MAGIC %md ### Save results + timings
# MAGIC
# MAGIC The timings CSV sits *alongside* the `spark_sql/` folder rather than
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

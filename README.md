# eCommerce Behaviour Big Data Analysis (IST3134 Group Assignment)

This project uses the [eCommerce behavior data from multi-category store](https://kaggle.com/datasets/mkechinov/ecommerce-behavior-data-from-multi-category-store)
dataset. The file used is `2020-Apr.csv.gz`, with roughly 66.6M events.

Five analyses, each implemented four ways, to compare environments and APIs.

| # | Script | API | Environment |
|---|--------|-----|-------------|
| 1 | `scripts/local/01_dataframe_api.py` | DataFrame API | Local |
| 2 | `scripts/local/02_spark_sql.py` | Spark SQL | Local |
| 3 | `scripts/databricks/01_dataframe_api.py` | DataFrame API | Databricks |
| 4 | `scripts/databricks/02_spark_sql.py` | Spark SQL | Databricks |

**Additional scripts**
- `scripts/local/00_prepare_data.py`, the one-time CSV to Parquet conversion, local (run before 1-2)
- `scripts/databricks/00_prepare_data.py`, the same conversion as a Databricks notebook (run before 3-4)
- `scripts/03_compare_results.py`, which combines all four timings.csv files into a comparison table and chart (run after 1-4)

Each of scripts 1-4 is self-contained, with its own `time_it` timing helper and its own
imports, rather than sharing a module. This is deliberate. The Databricks
notebooks (3-4) can't cleanly import a local Python module, so keeping every
script standalone means all four can be run, read, or pasted independently
without hunting through other files.

## Analyses

1. **Customer Activity Distribution** (count of events by `event_type`)
2. **Top 10 Most Purchased Products** (`product_id` ranked by purchase count)
3. **Most Popular Product Categories** (`category_code` ranked by interaction count)
4. **Most Popular Brands** (`brand` ranked by interaction count)
5. **Customer Purchase Trends** (daily purchase count across April 2020)

## Why convert to Parquet first (`00_prepare_data.py`)

The raw file is a single gzip-compressed CSV. Gzip isn't splittable, so Spark
reads it as one task regardless of core or executor count, which would make
every benchmark pay a fixed decode cost unrelated to the analysis itself.
Converting once to Parquet (columnar, splittable, compressed) gives all four
implementations a fair, realistic starting point. It also mirrors real-world
practice, where raw data is ingested once and analytics run many times
against an optimized format.

## Running locally

Run every command below from the repo root. The scripts use paths like
`results/...` and `data/...` relative to the current working directory.

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1. Convert raw CSV to Parquet (once)
spark-submit scripts/local/00_prepare_data.py --input data/raw/2020-Apr.csv.gz --output data/parquet

# 2. Run both local implementations
spark-submit scripts/local/01_dataframe_api.py --input data/parquet
spark-submit scripts/local/02_spark_sql.py --input data/parquet
```

Each script writes its results to `results/local/<implementation>/*.csv` and
its timings to `results/local/<implementation>_timings.csv`, mirroring the
`scripts/local` vs `scripts/databricks` split.

## Running on Databricks

1. Upload `2020-Apr.csv.gz` to a Unity Catalog Volume or DBFS path.
2. Import `scripts/databricks/00_prepare_data.py` as a notebook (File > Import).
   Databricks auto-detects the `# Databricks notebook source` header. Set
   the `input_path`/`output_path` widgets to your Volume paths, and Run All
   once to produce Parquet in the same Volume.
3. Import `scripts/databricks/01_dataframe_api.py` and
   `scripts/databricks/02_spark_sql.py` as notebooks (File > Import, choose
   the `.py` file). Databricks auto-detects the `# Databricks notebook
   source` header and cell markers.
4. Set the `input_path` widget to the Parquet location and Run All.
5. Each notebook writes its results and timings under
   `/Volumes/.../results/databricks/` on the Volume, in the same layout
   `scripts/03_compare_results.py` expects locally (`dataframe_api/`,
   `dataframe_api_timings.csv`, `spark_sql/`, `spark_sql_timings.csv`).
   Download that whole `results/databricks/` folder into your local
   `results/` folder. No renaming needed.

## Comparing results

```bash
python scripts/03_compare_results.py
```

Produces `results/comparison_table.csv` (execution time per analysis per
implementation, plus speedup ratios) and `results/comparison_chart.png`
(grouped bar chart).

## Repo layout

```
ecommerce-bigdata-analysis/
├── scripts/
│   ├── local/
│   │   ├── 00_prepare_data.py
│   │   ├── 01_dataframe_api.py
│   │   └── 02_spark_sql.py
│   ├── databricks/
│   │   ├── 00_prepare_data.py
│   │   ├── 01_dataframe_api.py
│   │   └── 02_spark_sql.py
│   └── 03_compare_results.py
├── requirements.txt
├── README.md
├── LICENSE
├── .gitignore
├── data/                  # gitignored, dataset too large for git
└── results/
    ├── local/
    │   ├── dataframe_api/             # 01-05 result CSVs
    │   ├── dataframe_api_timings.csv
    │   ├── spark_sql/                 # 01-05 result CSVs
    │   └── spark_sql_timings.csv
    ├── databricks/
    │   ├── dataframe_api/              # downloaded from the Volume
    │   ├── dataframe_api_timings.csv   # downloaded from the Volume
    │   ├── spark_sql/                  # downloaded from the Volume
    │   └── spark_sql_timings.csv       # downloaded from the Volume
    ├── comparison_table.csv
    └── comparison_chart.png
```

`.gitignore` already excludes `data/` and any local `.venv/`. Download the
dataset from the Kaggle link above instead of committing it.

## License

Released under the [MIT License](LICENSE).

"""
Combine the four timing CSVs into one comparison table + chart.

Run this LAST, after all four implementations have been executed and their
timings.csv files are collected into results/ locally, mirroring the
scripts/local vs scripts/databricks layout:

    results/local/dataframe_api_timings.csv        (from scripts/local/01_dataframe_api.py, produced automatically)
    results/local/spark_sql_timings.csv            (from scripts/local/02_spark_sql.py, produced automatically)
    results/databricks/dataframe_api_timings.csv   (from scripts/databricks/01_dataframe_api.py, download from the Volume)
    results/databricks/spark_sql_timings.csv       (from scripts/databricks/02_spark_sql.py, download from the Volume)

The Databricks scripts write to the same layout on the Volume, so just
download the whole results/databricks/ folder - no renaming needed.

Produces:
    results/comparison_table.csv   - one row per analysis, one column per implementation, plus speedup ratios
    results/comparison_chart.png   - grouped bar chart, seconds per analysis per implementation

Usage:
    python scripts/03_compare_results.py
"""
import matplotlib

# Must be set before pyplot is imported: this script only saves files, never
# opens a window, so it has to work on a headless machine.
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd

LOCAL_DF = "Local - DataFrame API"
LOCAL_SQL = "Local - Spark SQL"
DATABRICKS_DF = "Databricks - DataFrame API"
DATABRICKS_SQL = "Databricks - Spark SQL"

FILES = {
    LOCAL_DF: "results/local/dataframe_api_timings.csv",
    LOCAL_SQL: "results/local/spark_sql_timings.csv",
    DATABRICKS_DF: "results/databricks/dataframe_api_timings.csv",
    DATABRICKS_SQL: "results/databricks/spark_sql_timings.csv",
}

# derived column -> (numerator, denominator). Each is added only when both of
# its source columns are present, so partial runs still produce a table.
RATIOS = {
    "speedup_local_to_databricks_df": (LOCAL_DF, DATABRICKS_DF),
    "speedup_local_to_databricks_sql": (LOCAL_SQL, DATABRICKS_SQL),
    "ratio_local_df_to_sql": (LOCAL_DF, LOCAL_SQL),
    "ratio_databricks_df_to_sql": (DATABRICKS_DF, DATABRICKS_SQL),
}

TABLE_PATH = "results/comparison_table.csv"
CHART_PATH = "results/comparison_chart.png"


def load_timings():
    frames = []
    for impl_name, path in FILES.items():
        try:
            df = pd.read_csv(path)
        except FileNotFoundError:
            print(f"WARNING: {path} not found - skipping '{impl_name}'. "
                  f"Run that implementation (or download its timings.csv) first.")
            continue
        df["implementation"] = impl_name
        frames.append(df)
    if not frames:
        raise SystemExit(
            "No timing files found. Run the local and Databricks implementations first."
        )
    return pd.concat(frames, ignore_index=True)


def pivot_wide(long_df: pd.DataFrame) -> pd.DataFrame:
    wide = long_df.pivot(index="analysis", columns="implementation", values="seconds")
    return wide.reindex(sorted(wide.index), axis=0)


def add_speedup_ratios(wide: pd.DataFrame) -> pd.DataFrame:
    wide = wide.copy()
    for name, (numerator, denominator) in RATIOS.items():
        if {numerator, denominator}.issubset(wide.columns):
            wide[name] = wide[numerator] / wide[denominator]
    return wide


def plot_comparison(wide: pd.DataFrame, output_path: str):
    ax = wide.plot(kind="bar", figsize=(11, 6))
    ax.set_ylabel("Seconds (wall clock)")
    ax.set_xlabel("Analysis")
    ax.set_title("Execution time by analysis and implementation")
    ax.legend(title="Implementation", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"Chart written to {output_path}")


def main():
    wide = pivot_wide(load_timings())

    table = add_speedup_ratios(wide)
    table.to_csv(TABLE_PATH)
    print("\nComparison table:")
    print(table.round(3).to_string())
    print(f"\nWritten to {TABLE_PATH}")

    plot_comparison(wide, CHART_PATH)


if __name__ == "__main__":
    main()

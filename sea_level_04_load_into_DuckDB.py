import marimo

__generated_with = "0.23.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    # Required librairies
    # ===================

    # from datetime import datetime
    from pathlib import Path
    # import zipfile

    # import cdsapi
    import duckdb
    # import earthkit.data as ek
    # import matplotlib as mpl
    # import matplotlib.pyplot as plt
    # import numpy as np
    # import pandas as pd
    # import xarray as xr
    import warnings


    # Optional librairies
    # ===================

    # # Use catppuccin-macchiato palette for matplolib if available
    # try:
    #     # Optional, install with `uv add 'catppuccin[pygments]>=2.5.0'
    #     import catppuccin
    # except ModuleNotFoundError:
    #     pass
    # else:
    #     mpl.style.use(catppuccin.PALETTE.macchiato.identifier)


    # Project librairy
    # ================
    # Shared variables
    from sea_level_shared import (
    #     cds_config_path,
    #     data_dir, img_dir, nc_dir,
        data_dir,
    #     dataset_name,
    #     zip_file,
    )


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # 🌊 Sea Level Monitoring Pipeline
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    DuckDB Benefits:

        ✅ Zero setup - embedded SQLite-like database
        ✅ Fast - optimized for OLAP queries
        ✅ Local - no server needed
        ✅ SQL support - use standard SQL
        ✅ Parquet native - direct read/write
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 0 − Import Librairies & Set Up
    """)
    return


@app.cell
def _():
    # Set warnings level
    # ==================
    warnings.filterwarnings("ignore")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 1 − Initialize Database
    """)
    return


@app.cell
def _():
    # Initialize DuckDB database
    db_file = data_dir / 'sea_level_monitoring.duckdb'

    print(f"🗄️  Initializing DuckDB database...\n")

    # Connect to DuckDB (creates database if doesn't exist)
    conn = duckdb.connect(str(db_file))

    print(f"✅ Connected to DuckDB")
    print(f"   Database: {db_file}")
    print(
        f"   File size: {db_file.stat().st_size / 1024 / 1024:.2f} MB"
        if db_file.exists()
        else "   (new database)"
    )
    return (conn,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 2 − Populate `sea_level_monthly` table
    """)
    return


@app.cell
def _(conn):
    # Create table from Parquet file
    print("📥 Loading Parquet data into DuckDB…\n")

    parquet_file = data_dir / 'sea_level_data.parquet'

    try:
        # SQL command to create table from parquet
        create_table_sql = f"""
        CREATE TABLE sea_level_monthly AS
        SELECT * FROM read_parquet('{parquet_file}')
        """
    
        conn.execute(create_table_sql)
    
        # Get table info
        result = conn.execute(
            "SELECT COUNT(*) as row_count FROM sea_level_monthly"
        ).fetchall()
        row_count = result[0][0]
    
        print(f"✅ Table created: sea_level_monthly")
        print(f"   Rows: {row_count:,}")
        print(f"\n📊 Table schema:")
        schema = conn.execute(
            "DESCRIBE sea_level_monthly"
        ).fetchall()
        for col_name, col_type, *rest in schema:
            print(f"   • {col_name}: {col_type}")
        
    except Exception as e:
        print(f"⚠️  Table might already exist. Recreating...")
        conn.execute(
            "DROP TABLE IF EXISTS sea_level_monthly"
        )
        create_table_sql = f"""
        CREATE TABLE sea_level_monthly AS
        SELECT * FROM read_parquet('{parquet_file}')
        """
        conn.execute(create_table_sql)
        print(f"✅ Table recreated: sea_level_monthly")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 3 − Query examples
    """)
    return


@app.cell
def _(conn):
    # Example 1: Global statistics by year
    print("📊 Query 1: Global Mean Sea Level Anomaly by Year\n")

    query1 = """
    SELECT
        year,
        COUNT(*) as measurements,
        AVG(sea_level_anomaly_m) as mean_anomaly_m,
        MIN(sea_level_anomaly_m) as min_anomaly_m,
        MAX(sea_level_anomaly_m) as max_anomaly_m,
        STDDEV(sea_level_anomaly_m) as std_dev_m
    FROM sea_level_monthly
    GROUP BY year
    ORDER BY year
    """

    result1 = conn.execute(query1).fetchall()
    df_result1 = conn.execute(query1).df()
    print(df_result1.to_string(index=False))
    return


@app.cell
def _(conn):
    # Example 2: Highest and lowest sea level anomaly locations
    print("\n📊 Query 2: Locations with Extreme Sea Level Anomalies (Latest Month)\n")

    query2_high = """
    WITH latest AS (
        SELECT *
        FROM sea_level_monthly
        WHERE timestamp = (SELECT MAX(timestamp) FROM sea_level_monthly)
    ),
    ranked AS (
        SELECT
            latitude,
            longitude,
            sea_level_anomaly_m,
            timestamp,
            ROW_NUMBER() OVER (ORDER BY sea_level_anomaly_m DESC) AS rank_high,
            ROW_NUMBER() OVER (ORDER BY sea_level_anomaly_m ASC)  AS rank_low
        FROM latest
    )
    SELECT
        CASE
            WHEN rank_high <= 5 THEN 'Highest'
            ELSE 'Lowest'
        END AS anomaly_type,
        latitude,
        longitude,
        sea_level_anomaly_m,
        timestamp
    FROM ranked
    WHERE rank_high <= 5 OR rank_low <= 5
    ORDER BY anomaly_type, sea_level_anomaly_m DESC;
    """
    # SELECT
    #     'Highest' as anomaly_type,
    #     latitude,
    #     longitude,
    #     sea_level_anomaly_m,
    #     timestamp
    # FROM sea_level_monthly
    # WHERE timestamp = (SELECT MAX(timestamp) FROM sea_level_monthly)
    # ORDER BY sea_level_anomaly_m DESC
    # LIMIT 5
    # UNION ALL
    # SELECT
    #     'Lowest' as anomaly_type,
    #     latitude,
    #     longitude,
    #     sea_level_anomaly_m,
    #     timestamp
    # FROM sea_level_monthly
    # WHERE timestamp = (SELECT MAX(timestamp) FROM sea_level_monthly)
    # ORDER BY sea_level_anomaly_m ASC
    # LIMIT 5
    # """

    df_result2 = conn.execute(query2_high).df()
    print(df_result2.to_string(index=False))
    return


@app.cell
def _(conn):
    # Example 3: Regional analysis (choose a region)
    print("\n📊 Query 3: Mediterranean Sea Region (Sample)\n")

    # Mediterranean bounds: roughly 30-45°N, -6-42°E
    query3 = """
    SELECT
        timestamp,
        ROUND(AVG(sea_level_anomaly_m), 4) as mean_anomaly_m,
        COUNT(*) as grid_points
    FROM sea_level_monthly
    WHERE latitude BETWEEN 30 AND 45
      AND longitude BETWEEN -6 AND 42
    GROUP BY timestamp
    ORDER BY timestamp DESC
    LIMIT 12
    """

    df_result3 = conn.execute(query3).df()
    print(df_result3.to_string(index=False))
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

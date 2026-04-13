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
    ## Step 1 − Initialise Database
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
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

import marimo

__generated_with = "0.23.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    # Required librairies
    # ===================

    # from datetime import datetime
    from pathlib import Path
    import zipfile

    import cdsapi
    # import duckdb
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
        cds_config_path,
        data_dir, nc_dir,
        dataset_name,
        zip_file,
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
    ## Intro: Copernicus CDS → Parquet → DuckDB
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    **Project:** Download, explore, and analyze monthly sea level data

    **Tech Stack (via `uv`):**
    - `earthkit` - Access Copernicus CDS data
    - `xarray` & `pandas` - Data processing
    - `pyarrow` - Parquet format
    - `matplotlib` - Visualization
    - `duckdb` - Data warehouse (local, serverless)
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    **Expected project structure:**
    ```
    sea-level-monitoring/
    ├── pyproject.toml              # uv project config
    ├── uv.lock                     # Dependency lock file
    ├── .venv/                      # Virtual environment
    ├── sea_level_monitoring.ipynb  # This notebook
    └── data/                       # (created by this notebook)
        ├── sea_level_data.nc
        ├── sea_level_data.parquet
        └── sea_level_monitoring.duckdb
    ```
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
    ## Step 1 − Copernicus CDS API Setup
    """)
    return


@app.cell
def _():
    # Check if CDS API credentials exist
    print("🔐 Checking Copernicus CDS API credentials...\n")

    if cds_config_path.exists():
        print("✅ CDS API credentials found at ~/.cdsapirc")
        print("\n📋 Credentials file exists. You can proceed to download data.")
    else:
        print("⚠️  CDS API credentials NOT found.\n")
        print("📋 Setup instructions:")
        print("1. Go to: https://cds.climate.copernicus.eu/")
        print("2. Log in or create an account")
        print("3. Click your profile icon → Accept terms and conditions")
        print("4. Go to: https://cds.climate.copernicus.eu/profile")
        print("5. Copy your UID and API key")
        print("6. Create ~/.cdsapirc with:")
        print("")
        print("   url: https://cds.climate.copernicus.eu/api/v2")
        print("   key: YOUR_UID:YOUR_API_KEY")
        print("")
        print("7. Set file permissions: chmod 600 ~/.cdsapirc")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 2 − Get Seal Level Data
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Download data from Copernicus CDS¶

    https://cds.climate.copernicus.eu/datasets/satellite-sea-level-global?tab=overview

    We'll download monthly sea level anomaly data for 2019-2023
    """)
    return


@app.cell
def _():
    request_params = {
        'variable': 'monthly_mean',
        # 'year': ['2019', '2020', '2021', '2022', '2023',],
        # 'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
        'year': ['2019', '2020',],
        'month': ['01', '02', '03',],
        'version': 'vdt2024',
    }

    print('📥 Downloading sea level data...\n')
    print(f'Dataset: {dataset_name}')
    print(f'Period: {min(request_params["year"])}−{min(request_params["month"])}', end=' / ')
    print(f'{max(request_params["year"])}−{max(request_params["month"])}')
    print(f'Output: {zip_file}')
    print('\n⏳ This may take 2-5 minutes...\n')

    try:
        client = cdsapi.Client(timeout=120)
        client.retrieve(
            dataset_name,
            request_params,
            target=str(zip_file),
        )
        print('✅ Download complete!')

    except Exception as e:
        print(f'❌ Download failed: {type(e).__name__}')
        print(f'Error: {e}')
        print('\nTroubleshooting:')
        print('- Check ~/.cdsapirc exists and has correct format')
        print('- Run: chmod 600 ~/.cdsapirc')
        print('- Check internet connection')
        print('- Try smaller date range first')

    if zip_file.exists():
        size_mb = zip_file.stat().st_size / 1024 / 1024
        print(f'\n📁 Zip archive size: {size_mb:.1f} MiB')
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 3 − Extract Zip archive
    """)
    return


@app.cell
def _():
    print('📦 Extracting ZIP...\n')

    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            zip_ref.extractall(nc_dir)

        # List .nc files
        nc_files = sorted(list(nc_dir.glob('*.nc')))
        print(f'✅ Extraction complete!')
        print(f'\n📊 Files: {len(nc_files)}')
        for f in nc_files[:5]:  # Afficher les 5 premiers
            print(f'  - {f.name}')
        if len(nc_files) > 5:
            print(f'  … and {len(nc_files) - 5} other')

    

    except Exception as e:
        print(f'❌ Error: {e}')
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

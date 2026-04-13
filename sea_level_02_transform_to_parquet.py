import marimo

__generated_with = "0.23.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    # Required librairies
    # ===================

    # from datetime import datetime
    # from pathlib import Path
    # import zipfile

    # import cdsapi
    # import duckdb
    # import earthkit.data as ek
    # import matplotlib as mpl
    # import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import xarray as xr
    import warnings


    # Optional librairies
    # ===================

    # Use catppuccin-macchiato palette for matplolib if available
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
        data_dir, nc_dir,
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
    ## Step 1 − Load & Combine all NC files with `xarray`
    """)
    return


@app.cell
def _():
    # Load the downloaded NetCDF file with xarray
    nc_files = sorted(list(nc_dir.glob("*.nc")))

    print(f"🔓 Loading {len(nc_files)} NetCDF files…\n")

    datasets = []
    for nc_file in nc_files:
        try:
            ds = xr.open_dataset(nc_file, engine="netcdf4")
            datasets.append(ds)
            print(f"✅ {nc_file.name}")
        except Exception as e:
            print(f"❌ {nc_file.name}: {e}")

    if datasets:
        # Combine every dataset, sorted by time
        print("\n🔀 Combining datasets…")
        ds = xr.concat(datasets, data_vars="all", dim="time").sortby("time")
        print(f'✅ Combined dataset shape: {dict(ds.dims)}')
        print(f'\n{ds}')
    else:
        print("❌ No file loaded")
    return (ds,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 2 − Transform to Parquet format
    """)
    return


@app.cell
def _(ds):
    # Convert xarray Dataset to pandas DataFrame → Parquet
    print("🔄 Transforming NetCDF → Parquet…\n")

    if 'sla' in ds.data_vars:
        # Convert 3D (time, lat, lon)
        print(f"3D data: {ds['sla'].shape}")
        print('Conversion…\n')

        # Simple way: iterate dimensions
        data_list = []

        for t_idx, time_val in enumerate(ds['time'].values):
            for lat_idx, lat in enumerate(ds['latitude'].values):
                for lon_idx, lon in enumerate(ds['longitude'].values):
                    value = ds['sla'].values[t_idx, lat_idx, lon_idx]

                    # Step over NaNs
                    if not np.isnan(value):
                        data_list.append({
                            'timestamp': str(time_val),
                            'latitude': float(lat),
                            'longitude': float(lon),
                            'sea_level_anomaly_m': float(value)
                        })

            # Progress
            if (t_idx + 1) % 3 == 0:
                print(f"  Processed {t_idx + 1}/{len(ds['time'])}")

        # Create a DataFrame
        df = pd.DataFrame(data_list)

        # Add year/month
        df['year'] = pd.to_datetime(df['timestamp']).dt.year
        df['month'] = pd.to_datetime(df['timestamp']).dt.month

        # Reorder
        df = df[['timestamp', 'year', 'month', 'latitude', 'longitude', 'sea_level_anomaly_m']]

        print(f'\n📊 Shape : {df.shape}')
        print(f'Rows: {len(df):,}')
        print(f'\n🔍 Example:')
        print(df.head(10))
        print(f'\nTypes:')
        print(df.dtypes)
    return (df,)


@app.cell
def _(df):
    # Export to a Parquet file
    parquet_file = data_dir / 'sea_level_data.parquet'
    df.to_parquet(parquet_file, compression='snappy', engine='pyarrow', index=False)

    print(f"\n✅ Parquet file created!")
    print(f"   Path: {parquet_file}")
    print(f"   Size: {parquet_file.stat().st_size / 1024 / 1024:.2f} MB")
    print(f"   Compression: Snappy")
    return


if __name__ == "__main__":
    app.run()

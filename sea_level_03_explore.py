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
    # import duckdb
    # import earthkit.data as ek
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    # import xarray as xr
    import warnings


    # Optional librairies
    # ===================

    # Use catppuccin-macchiato palette for matplolib if available
    try:
        # Optional, install with `uv add 'catppuccin[pygments]>=2.5.0'
        import catppuccin
    except ModuleNotFoundError:
        pass
    else:
        mpl.style.use(catppuccin.PALETTE.macchiato.identifier)


    # Project librairy
    # ================
    # Shared variables
    from sea_level_shared import (
    #     cds_config_path,
    #     data_dir, img_dir, nc_dir,
        data_dir, img_dir,
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
    ## Step 1 − Load Parquet file
    """)
    return


@app.cell
def _():
    # Read and verify the parquet file
    parquet_file = data_dir / 'sea_level_data.parquet'

    if parquet_file.exists():
        print("✅ Parquet file verification:\n")
    
        # Read parquet file
        df = pd.read_parquet(parquet_file)
        print(df.info())
    else:
        print(f"❌ File not found: {parquet_file}")
    return (df,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 2 − Data Exploration & Quality Check
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Basic statistics
    """)
    return


@app.cell
def _(df):
    dates = pd.to_datetime(dict(year=df['year'], month=df['month'], day=1))
    
    print(f"📊 Shape: {df.shape}")
    print(f"\n📋 Data types:\n{df.dtypes}")
    print(f"\n🔢 First 5 rows:\n{df.head(5)}")

    print(f"\n📊 Summary statistics:")
    print(df[['latitude', 'longitude', 'sea_level_anomaly_m']].describe())

    print("\n📅 Date range:")
    print(f"   From: {dates.min().to_period('M')}")
    print(f"   To: {dates.max().to_period('M')}")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Missing values report
    """)
    return


@app.cell
def _(df):
    report = pd.DataFrame({
        'missing_count': df.isna().sum().values,
        'missing_rate (%)': df.isna().mean() * 100
    }).sort_values(by='missing_rate (%)', ascending=False)

    print("")
    print(report)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 3 − Data Viz’
    """)
    return


@app.cell
def _():
    mo.md(r"""
    ### Latest Sea Level Anomaly Map
    """)
    return


@app.cell
def _(df):
    # Visualize latest sea level anomaly map

    # Keep only the latest timestamp (using year + month)
    latest_year = df["year"].max()
    latest_month = df[df["year"] == latest_year]["month"].max()
    df_latest = df[(df["year"] == latest_year) & (df["month"] == latest_month)]

    # --- Build the 2D grid (lon × lat) ---
    lon = np.sort(df_latest["longitude"].unique())
    lat = np.sort(df_latest["latitude"].unique())

    # Pivot to a 2D array: rows = lat, cols = lon
    grid = df_latest.pivot(index="latitude", columns="longitude", values="sea_level_anomaly_m").values

    # --- Plot ---
    fig, ax = plt.subplots(figsize=(14, 8))

    # Compute robust color limits (2nd–98th percentile)
    vmin, vmax = np.nanpercentile(grid, [2, 98])
    # Center the diverging colormap symmetrically around 0
    abs_max = max(abs(vmin), abs(vmax))

    im = ax.pcolormesh(
        lon, lat, grid,
        cmap="RdBu_r",
        vmin=-abs_max,
        vmax=abs_max,
        shading="auto",
    )

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Sea Level Anomaly (m)")

    ax.set_title(
        f"Global Sea Level Anomaly – {latest_year}-{latest_month:02d}",
        fontsize=14, fontweight="bold",
    )
    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")

    plt.tight_layout()

    # Save the map
    map_file = img_dir / 'sea_level_map_latest.png'
    plt.savefig(map_file, dpi=300, bbox_inches='tight')

    plt.show()
    # Return the figure so Marimo renders it as a cell output
    # mo.mpl.interactive(fig)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Times series: Global Mean Sea Level Anomaly
    """)
    return


@app.cell
def _(df):
    # --- Compute global spatial mean per (year, month) ---
    global_mean = (
        df.groupby(["year", "month"])["sea_level_anomaly_m"]
        .mean()
        .reset_index()
        .sort_values(["year", "month"])
    )

    # Build a proper datetime index for the x-axis
    global_mean["date"] = pd.to_datetime(
        global_mean[["year", "month"]].assign(day=1)
    )

    # --- Plot ---
    fig2, ax2 = plt.subplots(figsize=(13, 6))

    # Main line with markers
    ax2.plot(
        global_mean["date"],
        global_mean["sea_level_anomaly_m"],
        linewidth=2.5,
        marker="o",
        markersize=5,
        color="steelblue",
        label="Global mean SLA",
    )

    # Add linear trend line
    x_numeric = np.arange(len(global_mean))
    z = np.polyfit(x_numeric, global_mean["sea_level_anomaly_m"], 1)
    p = np.poly1d(z)

    ax2.plot(
        global_mean["date"],
        p(x_numeric),
        "r--",
        linewidth=2,
        label="Trend",
    )

    ax2.set_title(
        "Global Mean SLA (Sea Level Anomaly) Over Time",
        fontsize=14, fontweight="bold",
    )
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Sea Level Anomaly (m)")
    ax2.grid(True, alpha=0.3)
    ax2.legend()

    plt.tight_layout()

    # Save the time series
    ts_file = img_dir / 'sea_level_timeseries.png'
    plt.savefig(ts_file, dpi=300, bbox_inches='tight')

    plt.show()

    # # Return the figure so Marimo renders it as a cell output
    # mo.mpl.interactive(fig2)
    return


if __name__ == "__main__":
    app.run()

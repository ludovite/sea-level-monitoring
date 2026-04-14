import marimo

__generated_with = "0.23.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo


    # Required librairies
    # ===================
    from pathlib import Path
    import zipfile

    import cdsapi
    import warnings

    from sea_level_shared import (
        cds_config_path,
        data_dir,
        sst_dataset_name,
        sst_zip_file,
        sst_nc_dir,
    )


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # 🌡️ Sea Surface Temperature Pipeline
    ## Copernicus CDS → Parquet → DuckDB
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    **Dataset:** [Global sea surface temperature derived from satellite observations](https://cds.climate.copernicus.eu/datasets/satellite-sea-surface-temperature)

    **Source:** ESA SST CCI / C3S — reprocessed L4, 0.05° grid, gap-free
    **DOI:** [10.24381/cds.cf608234](https://doi.org/10.24381/cds.cf608234)
    **Licence:** CC-BY (SST CCI datasets licence)

    Same temporal coverage as the sea level dataset for cross-analysis.
    """)
    return


@app.cell
def _():
    warnings.filterwarnings("ignore")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 1 − CDS API credentials check
    """)
    return


@app.cell
def _():
    # Réutilise la même logique que le pipeline sea level :
    # les credentials CDS sont partagés entre tous les datasets.
    print("🔐 Checking Copernicus CDS API credentials...\n")
    if cds_config_path.exists():
        print("✅ CDS API credentials found at ~/.cdsapirc")
    else:
        print("⚠️  CDS API credentials NOT found.")
        print("See the sea_level_01_extract notebook for setup instructions.")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 2 − Download SST data

    Test subset: Q1 2019 + Q1 2020, monthly, global coverage.
    `variable='all'` retrieves SST + ancillary fields (uncertainty, sea ice, masks).
    Narrow it down later for production.
    """)
    return


@app.cell
def _():
    request_params = {
        "variable": "all",
        "version": "3_0",
        "processinglevel": "level_4",
        "sensor_on_satellite": "combined_product",
        "temporal_resolution": "monthly",
        # 'year': ['2019', '2020', '2021', '2022', '2023'],
        # 'month': ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'],
        "year": ["2019", "2020"],
        "month": ["01", "02", "03"],
    }

    print("📥 Downloading SST data...\n")
    print(f"Dataset: {sst_dataset_name}")
    print(
        f"Period: {min(request_params['year'])}−{min(request_params['month'])}",
        end=" / ",
    )
    print(f"{max(request_params['year'])}−{max(request_params['month'])}")
    print(f"Output: {sst_zip_file}")
    print("\n⏳ This may take 2-5 minutes...\n")

    try:
        client = cdsapi.Client(timeout=120)
        client.retrieve(
            sst_dataset_name,
            request_params,
            target=str(sst_zip_file),
        )
        print("✅ Download complete!")
    except Exception as e:
        print(f"❌ Download failed: {type(e).__name__}")
        print(f"Error: {e}")
        print("\nTroubleshooting:")
        print("- Check ~/.cdsapirc exists and has correct format")
        print("- Run: chmod 600 ~/.cdsapirc")
        print("- Check internet connection")
        print("- Try smaller date range first")

    if sst_zip_file.exists():
        size_mb = sst_zip_file.stat().st_size / 1024 / 1024
        print(f"\n📁 Zip archive size: {size_mb:.1f} MiB")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 3 − Extract ZIP archive
    """)
    return


@app.cell
def _():
    print("📦 Extracting ZIP...\n")
    try:
        with zipfile.ZipFile(sst_zip_file, "r") as zip_ref:
            zip_ref.extractall(sst_nc_dir)

        nc_files = sorted(list(sst_nc_dir.glob("*.nc")))
        print("✅ Extraction complete!")
        print(f"\n📊 Files: {len(nc_files)}")
        for f in nc_files[:5]:
            print(f"  - {f.name}")
        if len(nc_files) > 5:
            print(f"  … and {len(nc_files) - 5} other")
    except Exception as e:
        print(f"❌ Error: {e}")
    return (nc_files,)


@app.cell
def _(nc_files):
    import xarray as xr

    # nc_files = sorted(sst_nc_dir.glob("*.nc"))
    ds = xr.open_dataset(nc_files[0])
    print(ds)
    print("\nVariables:", list(ds.data_vars))
    print("Dimensions:", dict(ds.sizes))
    print("\nSST attrs:", ds[list(ds.data_vars)[0]].attrs)
    return


if __name__ == "__main__":
    app.run()

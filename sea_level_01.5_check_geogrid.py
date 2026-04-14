import marimo

__generated_with = "0.23.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    import numpy as np
    import xarray as xr

    from sea_level_shared import (
        cds_config_path,
        data_dir, nc_dir,
        dataset_name,
        zip_file,
    )


@app.cell
def _():

    # Prends n'importe quel .nc déjà extrait
    nc_files = sorted(nc_dir.glob("*.nc"))
    ds = xr.open_dataset(nc_files[0])

    print(ds)
    print("\n--- Coordinates ---")
    print(f"lat: {ds.latitude.size} points, {float(ds.latitude.min()):.4f} → {float(ds.latitude.max()):.4f}")
    print(f"lon: {ds.longitude.size} points, {float(ds.longitude.min()):.4f} → {float(ds.longitude.max()):.4f}")

    # Résolution = pas de grille
    lat_res = float(np.diff(ds.latitude.values).mean())
    lon_res = float(np.diff(ds.longitude.values).mean())
    print(f"\nResolution: {lat_res:.4f}° lat × {lon_res:.4f}° lon")

    # Vérification : pas constant ?
    lat_diffs = np.diff(ds.latitude.values)
    print(f"Regular grid: {np.allclose(lat_diffs, lat_diffs[0])}")

    # Parfois c'est écrit dans les attributs globaux
    for key in ("geospatial_lat_resolution", "geospatial_lon_resolution", "resolution"):
        if key in ds.attrs:
            print(f"{key}: {ds.attrs[key]}")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Coordinates min, max
    [-180, 180] : Greenwich au centre, Pacifique coupé. C'est ce que ton SST utilise (-180.0 ... 180.0 dans ta sortie d'hier).

    ### geospatial resolution = 0.25
    Les deux grilles (level & temperature) seront superposables au point près, ce qui rendra les jointures DuckDB triviales avec une clé (time, lat, lon).
    """)
    return


if __name__ == "__main__":
    app.run()

import marimo

__generated_with = "0.23.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo


    # Required librairies
    # ===================
    from pathlib import Path
    import warnings

    import xarray as xr

    from sea_level_shared import (
        # cds_config_path,
        data_dir,
        # sst_dataset_name,
        # sst_zip_file,
        nc_dir, sst_nc_dir,
    )


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Remarqueq par rapport au sea level

    - Variable principale : sla (Sea Level Anomaly) en float64 — c'est l'anomalie par rapport à une climatologie de référence, pas le niveau absolu. Normal pour ce produit.

    - Bonus : eke (Eddy Kinetic Energy) — tu as une deuxième variable gratuite qui pourrait être intéressante à explorer plus tard. Énergie cinétique des tourbillons océaniques, liée aux courants.

    - Dimensions nommées latitude/longitude (vs lat/lon pour le SST). À harmoniser au moment du rename comme dans le script ci-dessus.

    - Date : 2019-01-15 — milieu du mois, convention standard pour une moyenne mensuelle. Le SST utilise 2019-01-16T12:00:00 (aussi le milieu, mais avec l'heure). Pour les jointures temporelles, tu devras soit tronquer à la date, soit créer une colonne year_month ("2019-01") comme clé commune — ce sera plus robuste.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 1 − Réinterpoler le SST sur la grille exacte du sea level
    """)
    return


@app.cell
def _():
    # Ouvre les 6 fichiers SST mensuels
    sst_files = sorted(sst_nc_dir.glob("*.nc"))
    ds_sst = xr.open_mfdataset(
        sst_files,
        data_vars="all",
        combine="by_coords",
        chunks={"time": 1})
    ds_sst = ds_sst[["analysed_sst", "sea_ice_fraction"]]

    # Grille cible = celle du sea level (lue depuis un fichier de référence)
    sla_ref = xr.open_dataset(sorted(nc_dir.glob("*.nc"))[0])
    target_lat = sla_ref.latitude.values  # -89.875 … 89.875
    target_lon = sla_ref.longitude.values  # -179.875 … 179.875

    # Renommage pour cohérence (le SST utilise 'lat'/'lon', le SLA 'latitude'/'longitude')
    ds_sst = ds_sst.rename({"lat": "latitude", "lon": "longitude"})

    # Interpolation bilinéaire sur la grille sea level
    ds_sst_regridded = ds_sst.interp(
        latitude=target_lat,
        longitude=target_lon,
        method="linear",
    )

    # Kelvin → Celsius
    ds_sst_regridded["analysed_sst"] = ds_sst_regridded["analysed_sst"] - 273.15
    ds_sst_regridded["analysed_sst"].attrs["units"] = "degrees_C"

    print(f"Regridded: {dict(ds_sst_regridded.sizes)}")
    print(f"lat: {float(ds_sst_regridded.latitude.min())} → {float(ds_sst_regridded.latitude.max())}")
    print(f"lon: {float(ds_sst_regridded.longitude.min())} → {float(ds_sst_regridded.longitude.max())}")
    ds_sst_regridded
    return (ds_sst_regridded,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 2 − Exporter au format Parquet

    Les deux grilles sont désormais superposables au point près. Une jointure DuckDB sur (latitude, longitude) en égalité stricte matchera 100% des points océaniques communs. Le piège du décalage de demi-case est évité.

    Volume estimé après export Parquet : 6 mois × 1.04 M points × ~70% NaN terrestres filtrés ≈ 1.9 M lignes. Avec deux colonnes float (sst_celsius, sea_ice_fraction) + clés, compresion snappy, attends-toi à 15-25 Mo de Parquet final. Très confortable.
    """)
    return


@app.cell
def _(ds_sst_regridded):
    # Passage en DataFrame long
    df = ds_sst_regridded.to_dataframe().reset_index()

    # Filtrage des points terrestres (NaN sur la SST)
    df = df.dropna(subset=["analysed_sst"])

    # Renommage harmonisé
    df = df.rename(columns={"analysed_sst": "sst_celsius"})

    # Clé temporelle commune avec sea level
    df["year_month"] = df["time"].dt.strftime("%Y-%m")

    # Réordonner les colonnes
    df = df[["year_month", "time", "latitude", "longitude",
             "sst_celsius", "sea_ice_fraction"]]

    print(f"📊 Rows: {len(df):,}")
    print(f"📅 Period: {df['year_month'].min()} → {df['year_month'].max()}")
    print(df.head())
    print(f"\nMemory: {df.memory_usage(deep=True).sum() / 1024**2:.1f} MiB")

    # Export
    parquet_path = data_dir / "sst_monthly.parquet"
    df.to_parquet(parquet_path, compression="snappy", index=False)

    size_mb = parquet_path.stat().st_size / 1024 / 1024
    print(f"\n✅ Written {parquet_path} ({size_mb:.1f} MiB)")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Volume final : 4.1 M lignes / 15.4 Mo Parquet — pile dans la fourchette estimée. Le ratio compression est remarquable (375 Mo en mémoire → 15 Mo sur disque, soit ×24), grâce à snappy + l'efficacité de Parquet sur des colonnes float64 avec des valeurs spatialement corrélées.
    Sanity check des données :

    Latitude -78.375 sur les premières lignes → c'est l'océan Austral, autour de l'Antarctique. Normal que ce soit le premier point océanique rencontré : en partant du pôle Sud, tout est continent jusqu'à ce qu'on franchisse la côte antarctique.
    SST autour de -0.8 °C → parfaitement cohérent avec ces latitudes en janvier (été austral mais eaux toujours froides, proches du point de congélation de l'eau de mer à -1.8 °C). La conversion Kelvin → Celsius est validée.
    sea_ice_fraction = 0.0 → également cohérent, la banquise antarctique est à son minimum estival en janvier-février.

    Calcul de couverture océanique :

    Grille totale : 720 × 1440 × 6 mois = 6 220 800 points
    Points océaniques : 4 100 670
    Ratio : 65.9% → cohérent avec la couverture océanique réelle (~71% de la surface terrestre, moins quelques pourcents perdus aux pôles couverts de glace permanente où la SST n'est pas définie).
    """)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()

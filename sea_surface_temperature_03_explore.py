import marimo

__generated_with = "0.23.0"
app = marimo.App(width="medium")

with app.setup:
    import marimo as mo

    from pathlib import Path
    import matplotlib as mpl
    import matplotlib.pyplot as plt
    import numpy as np
    import pandas as pd
    import warnings

    try:
        import catppuccin
    except ModuleNotFoundError:
        pass
    else:
        mpl.style.use(catppuccin.PALETTE.macchiato.identifier)

    from sea_level_shared import (
        data_dir,
        img_dir,
    )


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # 🌡️ Sea Surface Temperature Pipeline
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
    parquet_file = data_dir / "sst_monthly.parquet"

    if parquet_file.exists():
        print("✅ Parquet file verification:\n")
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
    dates = pd.to_datetime(dict(year=df["year"], month=df["month"], day=1))

    print(f"📊 Shape: {df.shape}")
    print(f"\n📋 Data types:\n{df.dtypes}")
    print(f"\n🔢 First 5 rows:\n{df.head(5)}")

    print(f"\n📊 Summary statistics:")
    print(df[["latitude", "longitude", "sst_celsius", "sea_ice_fraction"]].describe())

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
    report = pd.DataFrame(
        {
            "missing_count": df.isna().sum().values,
            "missing_rate (%)": df.isna().mean() * 100,
        }
    ).sort_values(by="missing_rate (%)", ascending=False)

    print("")
    print(report)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Step 3 − Data Viz’
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Latest Sea Surface Temperature Map
    """)
    return


@app.cell
def _(df):
    # Visualize latest SST map

    # Keep only the latest timestamp
    latest_year = df["year"].max()
    latest_month = df[df["year"] == latest_year]["month"].max()
    df_latest = df[(df["year"] == latest_year) & (df["month"] == latest_month)]

    # --- Build the 2D grid (lon × lat) ---
    lon = np.sort(df_latest["longitude"].unique())
    lat = np.sort(df_latest["latitude"].unique())

    grid = df_latest.pivot(
        index="latitude", columns="longitude", values="sst_celsius"
    ).values

    # --- Plot ---
    fig, ax = plt.subplots(figsize=(14, 8))

    # Robust color limits (2nd–98th percentile)
    # Pas de symétrisation : la SST n'est pas une anomalie centrée sur 0
    vmin, vmax = np.nanpercentile(grid, [2, 98])

    im = ax.pcolormesh(
        lon,
        lat,
        grid,
        cmap="RdYlBu_r",  # palette séquentielle thermique
        vmin=vmin,
        vmax=vmax,
        shading="auto",
    )

    cbar = fig.colorbar(im, ax=ax)
    cbar.set_label("Sea Surface Temperature (°C)")

    ax.set_title(
        f"Global Sea Surface Temperature – {latest_year}-{latest_month:02d}",
        fontsize=14,
        fontweight="bold",
    )
    ax.set_xlabel("Longitude (°)")
    ax.set_ylabel("Latitude (°)")

    plt.tight_layout()

    map_file = img_dir / "sst_map_latest.png"
    plt.savefig(map_file, dpi=300, bbox_inches="tight")

    plt.show()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Times series: Global Mean Sea Surface Temperature
    """)
    return


@app.cell
def _(df):
    # --- Compute area-weighted global mean per (year, month) ---
    # Pondération par cos(latitude) pour corriger la déformation de la grille
    df_w = df.copy()
    df_w["weight"] = np.cos(np.radians(df_w["latitude"]))
    df_w["sst_weighted"] = df_w["sst_celsius"] * df_w["weight"]

    global_mean = (
        df_w.groupby(["year", "month"])
        .apply(lambda g: g["sst_weighted"].sum() / g["weight"].sum())
        .reset_index(name="sst_celsius")
        .sort_values(["year", "month"])
    )

    global_mean["date"] = pd.to_datetime(global_mean[["year", "month"]].assign(day=1))

    # --- Plot ---
    fig2, ax2 = plt.subplots(figsize=(13, 6))

    ax2.plot(
        global_mean["date"],
        global_mean["sst_celsius"],
        linewidth=2.5,
        marker="o",
        markersize=5,
        color="coral",
        label="Global mean SST (area-weighted)",
    )

    # Pas de trend line sur 6 mois : le signal est dominé par le cycle saisonnier,
    # une régression linéaire n'aurait pas de sens climatique sur cette durée.

    ax2.set_title(
        "Global Mean SST (Sea Surface Temperature) Over Time",
        fontsize=14,
        fontweight="bold",
    )
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Sea Surface Temperature (°C)")
    ax2.grid(True, alpha=0.3)
    ax2.legend()

    plt.tight_layout()

    ts_file = img_dir / "sst_timeseries.png"
    plt.savefig(ts_file, dpi=300, bbox_inches="tight")

    plt.show()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Remarques

    Différences assumées avec le notebook SLA, justifiées :

    | Élément | SLA | SST | Pourquoi |
    |---------|-----|-----|----------|
    | Palette | `RdBu_r` (diverging) | `RdYlBu_r` (séquentielle) | SLA centré sur 0, SST non |
    | `vmin`/`vmax` | symétrique autour de 0 | 2-98 percentile asymétrique | Idem |
    | Pondération moyenne | simple `.mean()` | pondérée `cos(lat)` | Plus rigoureux pour climat global |
    | Trend line | oui | non | Inutile sur 6 mois saisonniers |
    | Couleur série | `steelblue` | `coral` | Code couleur thermique |

    Note sur la pondération cos(lat) : ton notebook SLA utilise une moyenne simple, ce qui sur-représente les hautes latitudes. Pour le SLA c'est acceptable (l'anomalie est centrée sur 0 et le biais est faible), mais pour la SST c'est plus visible — sans pondération, la moyenne globale est tirée vers le bas de plusieurs degrés à cause de l'Antarctique sur-représenté. Tu peux réutiliser cette technique côté SLA pour ta mise en prod si tu veux uniformiser.
    """)
    return


if __name__ == "__main__":
    app.run()

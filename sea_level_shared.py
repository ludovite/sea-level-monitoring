"""
Shared variables
"""


# import marimo as mo
#
# # Required librairies
# # ===================
#
# from datetime import datetime
# from pathlib import Path
# import zipfile
#
# import cdsapi
# import duckdb
# import earthkit.data as ek
# import matplotlib as mpl
# import matplotlib.pyplot as plt
# import numpy as np
# import pandas as pd
# import xarray as xr
# import warnings
#
#
# # Optional librairies
# # ===================
#
# # Use catppuccin-macchiato palette for matplolib if available
# try:
#     # Optional, install with `uv add 'catppuccin[pygments]>=2.5.0'
#     import catppuccin
# except ModuleNotFoundError:
#     pass
# else:
#     mpl.style.use(catppuccin.PALETTE.macchiato.identifier)
#
#
# # Project library
# # ================
# # Shared variables
# from sea_level_shared import (
#     cds_config_path,
#     data_dir, img_dir, nc_dir,
#     dataset_name,
#     zip_file,
# )
#

from pathlib import Path


# Files & Folders Structure
# =========================

# Create data directory
data_dir = Path.cwd() / "data"
data_dir.mkdir(exist_ok=True)

# Zip downloaded file
zip_file = data_dir / "sea_level_data.nc.zip"

# Subfolder with nc extracted files
nc_dir = data_dir / "nc_files"
nc_dir.mkdir(exist_ok=True)

# Generated images folder
img_dir = Path.cwd() / "img"
img_dir.mkdir(exist_ok=True)

print(f"📁 Data directory: {data_dir.absolute()}/")
print(f"🖼️ Images directory: {img_dir.absolute()}/")

# CDS API credentials file
# ========================
cds_config_path = Path.home() / ".cdsapirc"

# Dataset Name
# ============
dataset_name = "satellite-sea-level-global"

# Sea Surface Temperature (SST) dataset
# =====================================
sst_dataset_name = "satellite-sea-surface-temperature"
sst_zip_file = data_dir / "sst_data.zip"
sst_nc_dir = data_dir / "sst_nc"
sst_nc_dir.mkdir(parents=True, exist_ok=True)

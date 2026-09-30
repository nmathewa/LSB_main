#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec  7 15:37:45 2025

@author: nalex2023
"""

## https://nsf-ncar-era5.s3.amazonaws.com/e5.oper.an.sfc/202406/e5.oper.an.sfc.128_015_aluvp.ll025sc.2024060100_2024063023.nc


import xarray as xr
import fsspec
import subprocess
import sys 
import requests
import aiohttp
import zarr
import dask

import xarray as xr

# Connect directly to the Google Cloud public bucket
# No authentication required for public data
ds = xr.open_zarr(
    "gs://gcp-public-data-arco-era5/co/single-level-reanalysis.zarr", 
    consolidated=True
)

print(ds)

#%%
# Example: Slice a specific time and region WITHOUT downloading the rest
# This only pulls the tiny chunks of data needed for this specific slice
slice_ds = ds['t2m'].sel(
    time="2024-06-01T12:00", 
    latitude=slice(30, 25), 
    longitude=slice(-85, -80)
)

print(slice_ds)


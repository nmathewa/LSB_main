#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  5 15:06:06 2025

@author: nalex2023
"""

import cdsapi
import xarray as xr
import pandas as pd
from urllib.request import urlopen
import os
import argparse
import sys

class ERA5LandDownloader:
    
    def __init__(self, dataset_name='reanalysis-era5-single-levels',
                 var=None, 
                 dates=None,
                 pressure_level=None,
                 area=[90, -180, -90, 180],
                 download_flag=False,
                 download_fol=None,
                 multifile=False
                ):
        
        # Ensure the directory exists
        if not os.path.exists(download_fol):
            try:
                os.makedirs(download_fol, exist_ok=True)
            except OSError as e:
                print(f"Error creating directory {download_fol}: {e}")
                
        # Create dataset specific sub-folder
        dataset_folder = os.path.join(download_fol, dataset_name)
        if not os.path.exists(dataset_folder):
            os.makedirs(dataset_folder, exist_ok=True)
            
        # self.download_file is the directory path for this dataset
        self.download_file = dataset_folder
        
        self.dataset_name = dataset_name
        self.var = var 
        self.dates = dates
        self.multifile=multifile
        
        # Default pressure levels logic
        if pressure_level is None:
             self.pressure_level = [
        "1", "2", "3",
        "5", "7", "10",
        "20", "30", "50",
        "70", "100", "125",
        "150", "175", "200",
        "225", "250", "300",
        "350", "400", "450",
        "500", "550", "600",
        "650", "700", "750",
        "775", "800", "825",
        "850", "875", "900",
        "925", "950", "975",
        "1000"
    ]
        else:
            self.pressure_level = pressure_level
            
        self.area = area 
        self.download_flag = download_flag
        self.client = cdsapi.Client()

    def _get_save_path(self, start_date):
        """
        Helper method to unify output filename format:
        dir + data type name_date(start date)_variablename.nc
        """
        # Ensure date is a string in YYYY-MM-DD format
        date_str = pd.to_datetime(start_date).strftime('%Y-%m-%d')
        
        # Construct filename: dir/dataset_name_date_var.nc
        filename = f"{self.dataset_name}_{date_str}_{self.var}.nc"
        return os.path.join(self.download_file, filename)

    def create_params(self, specific_dates=None):
        
        # 1. Determine which dates to use
        target_dates = specific_dates if specific_dates is not None else self.dates

        if not isinstance(target_dates, pd.DatetimeIndex):
            target_dates = pd.to_datetime(target_dates)
            if isinstance(target_dates, pd.Timestamp):
                target_dates = pd.DatetimeIndex([target_dates])

        # 2. Basic Parameters
        params = dict(
            format="netcdf",
            product_type="reanalysis",
            variable=self.var,
            area=self.area,
        )
            
        # 3. Extract Year, Month, Day
        params["year"] = sorted(list(set(target_dates.strftime("%Y"))))
        params["month"] = sorted(list(set(target_dates.strftime("%m"))))
        params["day"] = sorted(list(set(target_dates.strftime("%d"))))
            
        # 4. Monthly Means Logic
        if self.dataset_name in ["reanalysis-era5-single-levels-monthly-means", 
                                 "reanalysis-era5-pressure-levels-monthly-means",
                                 "reanalysis-era5-land-monthly-means"]:
            params["product_type"] = "monthly_averaged_reanalysis"
            params["time"] = "00:00"
        
        # 5. Pressure Level Logic
        if "pressure-levels" in self.dataset_name:
            params["pressure_level"] = self.pressure_level
            
        # 6. Hourly Logic
        if self.dataset_name in ['reanalysis-era5-pressure-levels',
                                 'reanalysis-era5-land',
                                 'reanalysis-era5-single-levels']:
            params['time'] = [f"{x:02d}:00" for x in range(24)]
    
        # 7. ERA5-Land Clean up
        if self.dataset_name == "reanalysis-era5-land":
            if "product_type" in params:
                params.pop("product_type")
            
        return params
            
    def download_in_chunks(self, chunk_by='D'):
        """Splits the requested dates and downloads them sequentially."""
        if isinstance(self.dates, list):
            dt_index = pd.to_datetime(self.dates)
        else:
            dt_index = self.dates 

        grouped = dt_index.groupby(dt_index.to_period(chunk_by))

        print(f"Starting download. Splitting request into {len(grouped)} chunks...")

        for period, dates_in_chunk in grouped.items():
            period_str = str(period)
            
            current_filename = self._get_save_path(start_date=dates_in_chunk[0])

            if os.path.exists(current_filename) and self.download_flag:
                print(f"Skipping {current_filename}, already exists.")
                continue

            print(f"Processing chunk: {period_str}...")
            
            params = self.create_params(specific_dates=dates_in_chunk)

            try:
                fl = self.client.retrieve(self.dataset_name, params)
                if self.download_flag:
                    fl.download(current_filename)
                    print(f" -> Saved: {current_filename}")
            except Exception as e:
                print(f"!! Error downloading chunk {period_str}: {e}")
    
    def get_era5(self):
        heavy_datasets = ['reanalysis-era5-pressure-levels', 'reanalysis-era5-land']
        time_diff = pd.to_datetime(self.dates[-1]) - pd.to_datetime(self.dates[0])
        
        if self.multifile:
            self.download_in_chunks() 
    
        elif time_diff.days > 400:
            self.download_in_chunks() 
        elif self.dataset_name in heavy_datasets:
            self.download_in_chunks() 
        else:
            filename = self._get_save_path(start_date=self.dates[0])
            
            if os.path.exists(filename) and self.download_flag:
                print(f"Skipping {filename}, already exists.")
                return 

            params = self.create_params() 
            fl = self.client.retrieve(self.dataset_name, params) 
            
            if self.download_flag:
                fl.download(filename)
                print(f" -> Saved: {filename}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Download ERA5 data from CDS.")

    # REQUIRED: --var now accepts multiple arguments (nargs='+')
    parser.add_argument("--var", type=str, nargs='+', required=False, help="Variable name(s) (space separated)")
    parser.add_argument("--start_date", type=str, required=False, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=False, help="End date (YYYY-MM-DD)")
    parser.add_argument("--download_fol", type=str, required=False, help="Output directory path")

    # OPTIONAL
    parser.add_argument("--dataset_name", type=str, default="reanalysis-era5-single-levels", help="CDS Dataset name")
    parser.add_argument("--area", type=float, nargs=4, default=[90, -180, -90, 180], help="Area bounds [N, W, S, E]")
    parser.add_argument("--dry_run", action="store_true", help="If set, prepares arguments but does not download")
    parser.add_argument("--force_multi", default=False,help='Force download in chunks from cdsapirc')
    args, unknown = parser.parse_known_args()

    # --- INTELLIGENT DEFAULT HANDLING ---
    if args.var is None:
        print(">> No arguments provided. Using MANUAL DEFAULTS (IDE Mode)...")
        # Example of multiple variables in default mode
        args.var = ["2m_temperature", "total_precipitation"]
        args.start_date = "2000-01-01"
        args.end_date = "2000-01-05"
        args.download_fol = "/Users/nalex2023/main/LSB_main/LSB_APP/Docker_files/projects/Manus/Datasets/ERA5_dsets/"
        args.dataset_name = "reanalysis-era5-single-levels"
    # ------------------------------------

    # Create date list
    try:
        date_list = list(pd.date_range(args.start_date, args.end_date, freq='D').astype(str))
    except Exception as e:
        print(f"Error parsing dates: {e}")
        sys.exit(1)

    print(f"Initializing Downloader for {args.dataset_name}...")
    print(f"Period: {args.start_date} to {args.end_date}")
    print(f"Output: {args.download_fol}")

    # --- LOOP OVER VARIABLES ---
    # args.var is now always a list, e.g., ['2m_temperature', 'total_precipitation']
    for current_var in args.var:
        print(f"\n--- Starting download for variable: {current_var} ---")
        
        downloader = ERA5LandDownloader(
            dataset_name=args.dataset_name,
            var=current_var,
            dates=date_list,
            area=args.area,
            download_flag=not args.dry_run, 
            download_fol=args.download_fol,
            multifile=args.force_multi
        )

        downloader.get_era5()
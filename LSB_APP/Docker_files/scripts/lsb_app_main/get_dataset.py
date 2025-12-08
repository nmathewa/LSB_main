#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  5 15:06:06 2025

@author: nalex2023
"""

import pandas as pd 
import os 
import cdsapi 
import sys


# 1. get the csv configure file
# 2. download the ERA-5 dataset from the time period 
# 3. download the ERA-5 Land dataset 

class ERA5LandDownloader:
    def __init__(self,args):
        """
        Initialize the CDS API Client and define target variables.
        """
        self.client = cdsapi.Client()
        configure = pd.read_csv(args[0])
        self.lat = configure['Latitude']
        self.lon = configure['Longitude']
        ####### ISSUES only support year wise #########
        self.st = pd.to_datetime(configure['Start Date']).year
        self.en = pd.to_datetime(configure['End Date']).year

        # Requirement 3: Updated variable list
        self.variables = [
            "2m_temperature",
            "surface_latent_heat_flux",
            "surface_net_solar_radiation",
            "surface_sensible_heat_flux",
            "10m_u_component_of_wind",
            "10m_v_component_of_wind",
        ]

    def _get_bounding_box(self, offset=10):
        """
        Requirement 4: Calculate area -10, +10 all sides.
        CDS format is [North, West, South, East].
        """
        north = self.lat + offset
        south = self.lat - offset
        east = self.lon + offset
        west = self.lon - offset
        
        return [north, west, south, east]

    def download_period(self, output_dir):
        """
        Requirement 1 & 2: Adaptable for periods and lat/lon.
        Requirement 3: Saves a single file per month containing all variables.
        """
        
        # Ensure output directory exists
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            print(f"Created directory: {output_dir}")

        area = self._get_bounding_box(self.lat, self.lon)
        print(f"Downloading for location ({self.lat}, {self.lon}) with BBox: {area}")

        # Generate lists for all days and hours once
        days = [str(d).zfill(2) for d in range(1, 32)]
        times = [f"{h:02d}:00" for h in range(24)]

        # Iterate through years
        for year in range(self.st, self.en + 1):
            # Iterate through months
            for month in range(1, 13):
                month_str = str(month).zfill(2)
                
                # output filename: e.g., ERA5_Land_2009_01.nc
                filename = f"ERA5_Land_{year}_{month_str}.netcdf.zip"
                filepath = os.path.join(output_dir, filename)

                if os.path.exists(filepath):
                    print(f"File already exists, skipping: {filename}")
                    continue

                print(f"Retrieving data for {year}-{month_str}...")

                try:
                    self.client.retrieve(
                        'reanalysis-era5-land',
                        {
                            'data_format': 'netcdf',
                            'variable': self.variables, # Pass all variables at once
                            'year': str(year),
                            'month': month_str,
                            'day': days,
                            'time': times,
                            'area': area,
                        },
                        filepath
                    )
                    print(f"Saved: {filepath}")
                except Exception as e:
                    print(f"Failed to download {year}-{month_str}: {e}")

# --- Usage Example ---
if __name__ == "__main__":
    # Settings
    #save_path = "/Users/nalex2023/main/LSB_main/LSB_APP/Docker_files/scripts/lsb_app_main"
    #target_lat = -8.5 
    #target_lon = 123.0
    #start_y = 2009
    #end_y = 2009
    
    # Initialize and run
    args = sys.argv[1:]
    downloader = ERA5LandDownloader(args=args)
    downloader.download_period()



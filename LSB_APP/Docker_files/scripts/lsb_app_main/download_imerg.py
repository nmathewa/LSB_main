#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  9 17:11:29 2025

@author: nalex2023
"""

import earthaccess
import xarray as xr
from pprint import pprint
import pandas as pd 
import argparse
import os

class get_ea_gpm:
    
    
    def __init__(self,area,start_time,end_time,
                 out_dir):
        
        self.area = area
        self.start_time = start_time
        self.end_time = end_time 
        self.out_dir = out_dir
        
        if os.path.exists(f'{out_dir}'):
            pass
        else:
            os.mkdir(f'{out_dir}')
        
        
    def stream_imerg_lists(self):
        
        
        results = earthaccess.search_data(
            short_name="GPM_3IMERGHH",
            version="07",
            temporal=(f"{self.start_time}T00:00:00Z", f"{self.end_time}T23:59:59Z"),
            cloud_hosted=True, # Optional: search for cloud-hosted data,
        )
        file_handlers = earthaccess.open(results)
        return file_handlers
    
    
    def download_data(self):
        auth = earthaccess.login()
        
        
        file_lists = self.stream_imerg_lists()
        
        all_times = pd.date_range(start=f'{self.start_time}',
                                 end=f'{self.end_time}',periods=len(file_lists)).astype(str)
        
        
        for fileid in range(len(file_lists)):
            lat1,lat2 = self.area[2],self.area[0]
            lon1,lon2 = self.area[1],self.area[3]
            
            dset = xr.open_dataset(file_lists[fileid],group='/Grid',engine='h5netcdf').sel(lat=slice(lat1,lat2),
                                                             lon=slice(lon1,lon2))
            
            #print(dset)
            #break
            filename = f'{self.out_dir}{all_times[fileid]}_IMEG07.nc'
            
            dset.to_netcdf(filename)
            dset.close()
            print(f'file saved {filename}')
        
        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="IMERG GPM earthaccess downloader")
    
    # REQUIRED: --var now accepts multiple arguments (nargs='+')
    parser.add_argument("--area", type=float, nargs=4, default=[90, -180, -90, 180], help="Area bounds [N, W, S, E]")
    parser.add_argument("--start_date", type=str, required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--download_fol", type=str, required=True, help="Output directory path")
    args, unknown = parser.parse_known_args()
    
    get_ea_gpm(area=args.area,
                     start_time=args.start_date,
                     end_time=args.end_date, 
                     out_dir=args.download_fol).download_data()
        
        

#%%





'''

#%%
# Download granules to local path
downloaded_files = earthaccess.download(
    results,
    local_path='.', # Change this string to download to a different path
)

#%%



ds = xr.open_dataset('/Users/nalex2023/main/LSB_main/LSB_APP/Docker_files/projects/Manus/Datasets/GPM/RS/V07/IMERG/IMERG-FR/2019/07/13/3B-HHR.MS.MRG.3IMERG.20250101-S190000-E192959.1140.V07B.HDF5',
                     engine='h5netcdf',group='/Grid')


ds.precipitation.plot(x='lon',y='lat')

'''



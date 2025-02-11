#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Dec 29 14:31:50 2024

@author: nmathewa
"""
import pandas as pd
import xarray as xr 
import os 
import glob 

from dask.distributed import Client



#%%

met_manus_loc = '/media/nmathewa/nma_backup/Datasets/Datasets/met_manus'

proc_interp = '/media/nmathewa/nma_backup/Datasets/Datasets/Processed_interpsonde/interpsonde_manus/*.nc'

def show_columns(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)  
        if isinstance(result, pd.DataFrame): 
            print(f"Created DataFrame with columns: {result.columns.tolist()}")
        else:
            print("The function did not return a DataFrame.")
        return result  
    return wrapper

@show_columns
def create_dft(in_dir,arm_data=True):
    files = glob.glob(in_dir+os.sep+'*.nc')
    if len(files) == 0:
        files = glob.glob(in_dir+os.sep+'*.cdf')
        if len(files) == 0:
            raise Exception('No files Found')
        else:
            pass
    else:
        pass
    
    dft = pd.DataFrame(files,columns=['filepath'])
    dft['filename'] = dft['filepath'].str.split(os.sep).str[-1]
    
    if arm_data:
        dft['datetime'] = pd.to_datetime(dft['filename'].str.split(
            '.',
            expand=True).iloc[:, 2] + dft['filename'].str.split(
                '.',  expand=True).iloc[:, 3],format='%Y%m%d%H%M%S')
    return dft



met_dsets = create_dft(met_manus_loc)

#%%



met_dset = xr.open_mfdataset(met_dsets['filepath'],chunks={'time':100},combine='nested')



#%%






#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec 27 14:35:26 2024

@author: nmathewa
"""
import xarray as xr 
import pandas as pd 
import matplotlib.pyplot as plt 
import metpy.calc as mcalc
import numpy as np

import glob
import os 



pbl_fol = '/media/nmathewa/nma_backup/Datasets/Datasets/manus_pbl_ht_radiosonde'

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




pbl_dft = create_dft(pbl_fol)

file_test = '/media/nmathewa/nma_backup/Datasets/Datasets/Processed_interpsonde/interpsonde_manus/*.nc'

dset = xr.open_mfdataset(file_test,chunks={"time":100})

dset['time'] = dset['time'] + pd.Timedelta('10 hour')


#%%
dset['angle'] = (90 - np.degrees(np.arctan2(-dset['u_wind'], -dset['v_wind']))) % 360


#%%

dset['angle'].sel(height=slice(0,2)).isel(time=slice(0,24*364)).plot(x='time',
                                          cmap='RdBu_r')





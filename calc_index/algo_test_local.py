#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 11 14:18:05 2025

@author: nmathewa
"""

import glob
import os
import xarray as xr 
import pandas as pd
import numpy as np

from dask.distributed import Client, LocalCluster

if __name__ == '__main__':
    cluster = LocalCluster("127.0.0.1:1111", workers=8, threads=8)



#%%
met_arm = '/media/nmathewa/nma_backup/Datasets/Datasets/met_manus'



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


dft_met = create_dft(met_arm).sort_values('datetime').set_index('datetime')



years = [1998,1999,2000]



dft_sub = dft_met[dft_met.index.year.isin(years)]


#%%

def prepro_met(ds):
    ds_su = ds[['wdir_vec_mean','wspd_vec_mean','rh_mean','temp_mean']]
    ds_re = ds_su.resample(time='1h').mean()
    
    return ds_re


met_dset = xr.open_mfdataset(dft_sub['filepath'].values,
                             preprocess=prepro_met,parallel=True)



#%%
hour_fols = f'/media/nmathewa/nma_backup/Datasets/Datasets/met_manus/hourly_yearly_met_manus/{years[0]}_{years[-1]}_manus.nc'

met_dset.to_netcdf(hour_fols)








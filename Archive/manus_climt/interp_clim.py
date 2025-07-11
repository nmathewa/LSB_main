#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 21 11:27:55 2024

@author: nmathewa
"""

import pandas as pd
import xarray as xr 
import glob 
import os


#%%

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
#%%

in_fol = '/media/nmathewa/nma_backup/Datasets/Datasets/intersonde_manus'

inter_dft = create_dft(in_fol,True)

test_days = inter_dft.loc[0:100].sort_values('datetime')


test_ds = xr.open_mfdataset(test_days['filepath'])


test_ds['time'] = test_ds['time'] + pd.Timedelta(hours=10)


dates = test_days['datetime']

dates_text = f'{dates.iloc[0].strftime("%Y-%m-%d")} to {dates.iloc[-1].strftime("%Y-%m-%d")}'



anom_uwind = test_ds['u_wind'] - test_ds['u_wind'].mean(dim='time')


#%%


test_ds['hour'] = anom_uwind.time.dt.hour

test_ds_wdir_diurnal = anom_uwind.groupby(test_ds['hour']).mean()



#%%

test_ds_wdir_diurnal.sel(height=slice(0,5)).plot(x='hour',y='height')




#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 27 12:45:05 2025

@author: nmathewa
"""

import xarray as xr 
import pandas as pd
import glob 
import pandas as pd 
import os
import dask
import numpy as np
import xarray as xr
from dask.distributed import Client

launch_stat = '/home/nmathewa/main/LSB_main/stats/sonde_with_data.csv'

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



dft_sonde = pd.read_csv(launch_stat)

dft_sonde['date'] = pd.to_datetime(dft_sonde['date'])


dft_sonde.set_index('date',inplace=True)

inter_data = '/media/nmathewa/nma_backup/Datasets/Datasets/intersonde_manus'

dft_inter = create_dft(inter_data)

dft_inter['datetime'] = dft_inter['datetime'].dt.round('H')
dft_inter.set_index('datetime',inplace=True)

dft_inter_sub = pd.merge(dft_sonde,dft_inter,left_index=True,
                         right_index=True,how='inner')


#%%


interp_dset = xr.open_mfdataset(dft_inter_sub['filepath_y'],combine='nested',
                                parallel=True)







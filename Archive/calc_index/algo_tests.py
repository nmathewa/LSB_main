#!/usr/bin/env/ .
# -*- coding: utf-8 -*-
"""
Created on Sat Feb  8 13:52:36 2025

@author: nmathewa
"""

from dask_jobqueue import SLURMCluster
import matplotlib.pyplot as plt 
import glob
import os
import xarray as xr 
import pandas as pd
import numpy as np


cluster = SLURMCluster(
    job_name="Climt2",          # --job-name
    cores=2,                     # Number of cores per task (adjust if needed)
    processes=2,                 # One process per task
    memory="20GB",               # --mem
    walltime="00:15:00",         # --time
    queue="short",               # --partition
    log_directory=".",           # Logs will be saved to the current directory
)


from dask.distributed import Client
cluster.scale(jobs=10)
client = Client(cluster)

client

print(client.dashboard_link)


#%%



met_arm = '/home1/nalex2023/Datasets/met_manus/'


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


dft_met = create_dft(met_arm).sort_values('datetime')

#%%


test = xr.open_dataset(dft_met.loc[0]['filepath'])[['wdir_vec_mean','wspd_vec_mean',
                                                    'org_precip_rate_mean',
                                                    'rh_mean','temp_mean']]


def prepro_met(ds):
    ds_sub = ds[['wdir_vec_mean','wspd_vec_mean','rh_mean','temp_mean']]
    ds_re = ds_sub.resample(time='1H').mean()
    
    return ds_re


met_dset = xr.open_mfdataset(dft_met['filepath'].values,
                             preprocess=prepro_met)








#%%

data_dir = '/home1/nalex2023/Datasets/interpsonde_manus/*.nc'

all_dsets = glob.glob(data_dir)

all_dsets


#%%

from dask.distributed import Client
cluster.scale(jobs=10)
client = Client(cluster)

client

print(client.dashboard_link)

#%%



dset = xr.open_mfdataset(all_dsets, combine='by_coords')
dset['time'] = dset['time'] + pd.Timedelta('10 hour')

#%%

dset['angle'] = (90 - np.degrees(np.arctan2(-dset['u_wind'], -dset['v_wind']))) % 360

dset['speed'] = np.sqrt((dset['u_wind'] * dset['u_wind']) + 
                        (dset['v_wind'] * dset['v_wind']))


#%%


dset['angle'].sel(height=slice(0,2)).isel(time=slice(0,24*364)).plot(x='time',
                                          cmap='RdBu_r')


#%%

from windrose import WindroseAxes
from matplotlib import cm


def draw_windrose(data,color=cm.Blues):
    ws = data['speed'].values
    wd = data['angle'].values
    ax = WindroseAxes.from_ax()
    ax.bar(wd,ws,normed=False,cmap=color)
    ax.set_legend()


def wind_rose_compare(data1,data2 ,cost_angle):
    ws1 = data1['speed'].values
    wd1 = data1['angle'].values
    ws2 = data2['speed'].values
    wd2 = data2['angle'].values
    
    ax = WindroseAxes.from_ax()
    
    ax.bar(wd1,ws1,normed=False,cmap=cm.Reds)
    ax.set_legend(loc='lower right')
    ax.bar(wd2,ws2,normed=False,cmap=cm.Blues)
    ax.set_legend(loc='lower left')



#%%

dset['hour'] = dset.time.dt.hour

dset_diurnal = dset.set_coords('hour').groupby('hour').mean()


#%%

#dset_diurnal.speed.sel(height=slice(0,2)).plot()



ws_data = dset_diurnal['speed'].sel(height=slice(3,4),hour=15).values

wd_data = dset_diurnal['angle'].sel(height=slice(3,4),hour=15).values


#%%
ax = WindroseAxes.from_ax()

ax.bar(wd_data,ws_data,normed=True,cmap=cm.Blues)

ax.set_legend()


#%%







#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 26 15:27:08 2024

@author: nmathewa
"""

import xarray as xr 
import pandas as pd 
import os 
import glob 


file_test = '/media/nmathewa/nma_backup/Datasets/Datasets/Processed_interpsonde/interpsonde_manus/*.nc'

dset = xr.open_mfdataset(file_test,chunks={"time":100})

dset['time'] = dset['time'] + pd.Timedelta('10 hour')


#%%



dset_daily = dset[['u_wind','v_wind']]



dset_upper = dset_daily.sel(height=slice(1,3)).mean(dim='height')

dset_lower = dset_daily.sel(height=slice(0,1)).mean(dim='height')

fig,ax = plt.subplots()
dset_upper.u_wind.plot(ax=ax)

dset_lower.u_wind.plot(ax=ax)



#%%

dset_test = dset.isel(time=slice(0,100)).compute()


#%%
import matplotlib.pyplot as plt
import numpy as np

single_dd = dset_daily.sel(height=slice(0,2))

x = single_dd.month.values
y = single_dd.height.values

X, Y = np.meshgrid(x,y)

fig,ax = plt.subplots()

u = single_dd.u_wind.values
v = single_dd.v_wind.values

ax.quiver(X,Y,u,v)



#%%
dset_diurnal.sel(height=slice(2,3)).mean(dim='height').v_wind.plot()

#%%

dset_diurnal.sel(height=slice(0,1)).mean(dim='height').v_wind.plot()




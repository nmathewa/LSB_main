#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 23 14:04:05 2024

@author: nmathewa
"""

import xarray as xr 
import pandas as pd
import os
import matplotlib.pyplot as plt
import cartopy.crs as crs
import cartopy.feature as cfeature
import dask as da
from dask.distributed import Client
import holoviews as hv
import numpy as np


test_data = xr.open_dataset('/media/nmathewa/nma_backup/Datasets/Datasets/era5_plevels_maritime/era5_plevels_200103maritime.nc')


chunked_data = test_data.chunk(chunks={'valid_time':100})

land_10m = cfeature.NaturalEarthFeature('physical', 'land', '10m',color='white',
                                        edgecolor='black') 

fig,ax = plt.subplots(1,1,subplot_kw={'projection':
                                      crs.PlateCarree()})

gl = ax.gridlines(draw_labels=True,linewidth=1,color='white',alpha=0.2,linestyle='--',zorder=5)

chunked_data.u.isel(valid_time=0).sel(pressure_level=1000).plot(ax=ax,transform=crs.PlateCarree())

ax.set_extent([142, 150, -5, 5], crs=crs.PlateCarree())
ax.add_feature(land_10m)



#%%
hourly_summary = chunked_data[['u','v']].groupby('valid_time.hour').mean()

hourly_summary['speed'] = np.sqrt((hourly_summary['u'] * hourly_summary['u']) + (hourly_summary['v'] * hourly_summary['v']))

hourly_summary['angle'] = np.arctan(hourly_summary['v']/hourly_summary['u'])




#%%

final_data_loc = hourly_summary[['v','u','speed']].compute()

#%%

final_data_box = hourly_summary[['v','u','speed']].sel(pressure_level=900).isel(hour=23)

"""
x = final_data.hour.values
y = final_data.pressure_level.values
u = final_data.u.values
v = final_data.v.values
"""

x= final_data_box.longitude.values
y = final_data_box.latitude.values
u = final_data_box.u.values
v = final_data_box.v.values


X,Y = np.meshgrid(y,x)



fig,ax = plt.subplots(figsize=(12,18),subplot_kw={'projection':
                                      crs.PlateCarree()})

    
ax.set_extent([142, 150, -5, 5], crs=crs.PlateCarree())

final_data_box.speed.plot(ax=ax,transform=crs.PlateCarree())

qq = ax.quiver(Y,X,u,v,cmap='jet',transform=crs.PlateCarree(),scale=100)


ax.coastlines()


ax.tick_params(axis='both', which='major', labelsize=23)


qk = ax.quiverkey(qq, 0.7, .89, U=(1/30)*5,label=r'5 $ms^{-1}$', labelpos='E',
                   coordinates='figure',fontproperties={'size': 10})


#%%
















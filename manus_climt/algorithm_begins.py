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


pbl_fol = '/media/nmathewa/nma_backup/Datasets/Datasets/manus_pbl_depth_arm'

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



"""
pbl_dft = create_dft(pbl_fol)

test = pbl_dft['filepath'].iloc[0]


dds = xr.open_mfdataset(pbl_dft['filepath'],chunks='auto',combine='nested')
"""




#%%

file_test = '/media/nmathewa/nma_backup/Datasets/Datasets/Processed_interpsonde/interpsonde_manus/*.nc'

dset = xr.open_mfdataset(file_test,chunks={"time":100})

dset['time'] = dset['time'] + pd.Timedelta('10 hour')


#%%
dset['angle'] = (90 - np.degrees(np.arctan2(-dset['u_wind'], -dset['v_wind']))) % 360


#%%

dset['angle'].sel(height=slice(0,2)).isel(time=slice(0,24*364)).plot(x='time',
                                          cmap='RdBu_r')



dset['speed'] = np.sqrt((dset['u_wind'] * dset['u_wind']) + 
                        (dset['v_wind'] * dset['v_wind']))


#%%


dset_sub = dset.sel(height=slice(0,4))#.sel(time=slice('2001-04-05','2001-04-10'))#.isel(time=slice(0,50))

#dset_sub = dset.groupby('time.hour').mean()


#%%

times = np.arange(18,24)



sel_dset = dset_sub#.sel(time=dset_sub.time.dt.hour.isin(times))

cost_angle = 90
alpha  = dset_sub.sel(height=0.01,method='nearest')#.sel(hour=slice(12,19))
beta = dset_sub.sel(height=slice(3,4)).mean(dim='height')#.sel(hour=slice(12,19))

wld = wind_low['angle'].values
wls = wind_low['speed'].values


wud = wind_heigh['angle'].values
wus = wind_heigh['speed'].values


from windrose import WindroseAxes
from matplotlib import cm
ax = WindroseAxes.from_ax()


ax.bar(wud,wus,normed=True,cmap=cm.Blues)
ax.bar(wld,wls,normed=True,cmap=cm.Reds)
ax.set_legend()



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
alpha  = sel_dset.sel(height=0.01,method='nearest').compute()#.sel(hour=slice(12,19))
beta = sel_dset.sel(height=slice(3,4)).mean(dim='height').compute()#.sel(hour=slice(12,19))


#%%

cost_angle = 0


alpha_fil = alpha.where((cost_angle-90 < alpha['angle']) &  
                        (alpha['angle']< cost_angle+90),drop=True)


opposite_cost = cost_angle + 180
beta_fil = beta.where((opposite_cost-90 < beta['angle']) & 
                      (beta['angle'] < opposite_cost+90),drop=True)



low_winds = alpha_fil['angle'] + 180
beta_fil2 = beta_fil.where((low_winds - 90 < beta_fil['angle']) & 
                           (beta_fil['angle'] < low_winds + 90),drop=True) 


mask = beta_fil2.time



alpha_fil2 = alpha_fil.reindex(time=mask)


wind_rose_compare(data1=beta_fil2,data2=alpha_fil2)


#%%



SBI = np.cos(alpha['angle'] - cost_angle) * np.cos((alpha['angle'] + 180) - beta_fil2['angle'])










#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 11 18:03:45 2025

@author: nalex2023
"""
import pandas as pd 
import glob 
import xarray as xr 
import matplotlib.pyplot as plt 

class plots_arm:
    
    def __init__(self,project_name,lat,lon,arm_directory):
        
        self.project_name = project_name 
        self.lat = lat 
        self.lon = lon 
        self.arm_directory = arm_directory

    def read_surface_hourly(self):
        arm_directory = self.arm_directory
        dset_all = xr.open_mfdataset(arm_directory+'*.cdf',combine='by_coords')

        dset_sub = dset_all[['temp_mean','rh_mean','atmos_pressure','org_precip_rate_mean']]

        dset_sub_hourly = dset_sub.resample(time='1h').mean().compute()


        dset_sub_hourly['atmos_pressure'] = dset_sub_hourly['atmos_pressure'] * 10  # convert to hPa

        return dset_sub_hourly
    

    def plot_init_surface(self):
        subset_hourly = self.read_surface_hourly()

        fig,ax = plt.subplots(4,1,figsize=(10,8),sharex=True,sharey=False)

        subset_hourly['temp_mean'].plot(ax=ax[0],color='#770000')

        subset_hourly['rh_mean'].plot(ax=ax[1],color='#f49028')

        subset_hourly['atmos_pressure'].plot(ax=ax[2],color='#084771')

        subset_hourly['org_precip_rate_mean'].plot(ax=ax[3],color='#1f77b4')

        ax[0].set_ylabel('Temp (C)')
        ax[1].set_ylabel('RH (%)')
        ax[2].set_ylabel('Pressure (hPa)')
        ax[3].set_ylabel('Precip Rate (mm/hr)')

        for a in ax:
            a.set_xlabel('Time')
            a.grid(True,alpha=0.3)

        return fig,ax
    
    def plot_interp(self):
        fig,ax = plt.subplots(figsize=(10,4))
        arm_directory = self.arm_directory
        dset_interp = xr.open_mfdataset(arm_directory+'*.nc',combine='by_coords')

        dset_interp_sub = dset_interp[['temp']]

        dset_interp_sub_hourly = dset_interp_sub.resample(time='1h').mean().compute()

        fig,ax = plt.subplots(2,1,figsize=(10,8),sharex=True,sharey=False)

        dset_interp_sub_hourly['temp'].plot(x='time',ax=ax,vmin=-30,vmax=30,cmap='RdYlBu_r',cbar_kwargs={'label':'Temperature (C)'})

        ax.set_ylim(0,10)
        ax.set_ylabel('Height (km)')
        ax.set_xlabel('Time')
        # y ticks every 1 km
        ax.set_yticks(np.arange(0,11,1))

        ax.set_ylabel('Temp (C)')

        for a in ax:
            a.set_xlabel('Time')
            a.grid(True,alpha=0.3)

        return fig,ax
    

        
    

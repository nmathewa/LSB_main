12#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Dec 11 18:03:45 2025

@author: nalex2023
"""
import cartopy.crs as crs 
import matplotlib.pyplot as plt
from cartopy.io.img_tiles import GoogleTiles
from cartopy.mpl.gridliner import LongitudeFormatter, LatitudeFormatter
import pandas as pd 


class plots_inital:
    
    def __init__(self,project_name,lat,lon,start,end,dfts_list):
        
        self.project_name = project_name 
        self.lat = lat 
        self.lon = lon 
        self.start = start
        self.end = end
        self.dfts_list = dfts_list  
    
    def plot_region(self):
        
        lon = self.lon
        lat = self.lat
        
        fig,ax = plt.subplots(subplot_kw={'projection': crs.PlateCarree()})
        
        
        tiler = GoogleTiles(style='satellite')

        ax.coastlines(zorder=2)

        ax.set_extent([lon-30, lon+30, lat-20, lat+20], crs=crs.PlateCarree())

        ax.add_image(tiler, 5,zorder=0)
        # add rectangle
        rect_extent = [lon-5, lon+5, lat-5, lat+5]
        ax.add_patch(plt.Rectangle((rect_extent[0], rect_extent[2]), 
                            rect_extent[1]-rect_extent[0], 
                            rect_extent[3]-rect_extent[2],
                            lainewidth=2, edgecolor='red', facecolor='none', zorder=3))


        gl = ax.gridlines(draw_labels=True,linewidth=1, color='gray', alpha=0.2, linestyle='--')

        gl.yformatter = LatitudeFormatter()
        gl.xformatter = LongitudeFormatter()
        gl.top_labels = False
        gl.right_labels = False
        
        return fig,ax
    
  
    
    
    
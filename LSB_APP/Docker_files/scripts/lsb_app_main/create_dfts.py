#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  9 22:39:48 2025

@author: nalex2023
"""

import pandas as pd 
import glob 
import os 
import argparse
import json

class set_up_dfts:
    
    
    def __init__(self,era_land_dir,era_p,era_single,imerg,arm_met,arm_interp):
        
        
        self.era_land = era_land_dir
        self.era_p = era_p
        self.era_single = era_single
        self.imerg = imerg 
        self.arm_met = arm_met 
        self.arm_interp = arm_interp
        
        
    def create_era_dft(self,era_dir):
            
        all_files = glob.glob(f'{era_dir}*.nc')
        
        if len(all_files) == 0:
            return pd.DataFrame()
        era_dft = pd.DataFrame(columns=['filepath'])
        era_dft['filepath'] = all_files 
            
        era_dft['filename'] = era_dft['filepath'].str.split(os.sep).str[-1]
            
        era_dft['datetime'] = era_dft['filename'].str.split('_').str[1]
        era_dft['datetime'] = pd.to_datetime(era_dft['datetime'],format='%Y-%m-%d')
            
        era_dft['variable'] = era_dft['filename'].str.extract(r'^[^_]+_[^_]+_(.+)\.nc$')
            
        return era_dft 
        
        
    def create_arm_dft(self,arm_dir,vap=False):
            
        if vap:
            arm_files = glob.glob(f'{arm_dir}*.nc')
        else:
            arm_files = glob.glob(f'{arm_dir}*.cdf')
                
        arm_dft = pd.DataFrame(columns=['filepath'])
        arm_dft['filepath'] = arm_files

        arm_dft['filename'] = arm_dft['filepath'].str.split(os.sep).str[-1]

        arm_dft['datetime'] = arm_dft['filename'].str.split('.').str[2]

        arm_dft['datetime'] = pd.to_datetime(arm_dft['datetime'],format='%Y%m%d')
            
        return arm_dft 
        
        
    def create_gpm_dft(self,gpm_dir):
        imerg_files = glob.glob(f'{gpm_dir}*.nc')

        imerg_dft = pd.DataFrame(columns=['filepath'])
        imerg_dft['filepath'] = imerg_files

        imerg_dft['filename'] = imerg_dft['filepath'].str.split(os.sep).str[-1]

        imerg_dft["datetime"] = imerg_dft["filename"].str.extract(r"^([\d\-]+ [\d:\.]+)")

        imerg_dft["datetime"] = pd.to_datetime(imerg_dft['datetime'],format='%Y-%m-%d %H:%M:%S.%f')
        imerg_dft['datetime'] = imerg_dft['datetime'].dt.round('30min')
            
        return imerg_dft
        
    def create_dfts(self):
            
        era_land_dft = self.create_era_dft(era_dir=self.era_land)
        era_land_dft.to_csv(f'{self.era_land}era_land_dft.csv')
            
        era_p_dft = self.create_era_dft(era_dir=self.era_p)
        era_p_dft.to_csv(f'{self.era_p}era_p_dft.csv')
            
        era_single_dft = self.create_era_dft(era_dir=self.era_single)
        era_single_dft.to_csv(f'{self.era_single}era_land_dft.csv')
            
        arm_met_dft = self.create_arm_dft(arm_dir=self.arm_met,vap=False)
        arm_met_dft.to_csv(f'{self.arm_met}arm_met_dft.csv')
            
            
        arm_interp_dft = self.create_arm_dft(arm_dir=self.arm_interp,vap=True)
        arm_interp_dft.to_csv(f'{self.arm_interp}arm_interp_dft.csv')
            
        imerg_dft = self.create_gpm_dft(gpm_dir=self.imerg)
        imerg_dft.to_csv(f'{self.imerg}imerg_dft.csv')
        
        locations = {
                    "era_land_dft": f'{self.era_land}era_land_dft.csv',   # path where DFTs were created
                        "era_p_dft": f'{self.era_p}era_p_dft.csv',
                        "era_single_dft":f'{self.era_single}era_land_dft.csv',
                        "arm_met_dft":f'{self.arm_met}arm_met_dft.csv',
                        "arm_interp_dft" : f'{self.arm_interp}arm_interp_dft.csv',
                        "imerg_dft" : f'{self.imerg}imerg_dft.csv'
                            }
        
        print(json.dumps(locations))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Data DFT constructor")
    
    # REQUIRED: --var now accepts multiple arguments (nargs='+')
    parser.add_argument("--era_land_dir", type=str,required=True, help="ERA-5 Land directory")
    parser.add_argument("--era_p_dir", type=str,required=True, help="ERA-5 Land directory")
    parser.add_argument("--era_surface_dir", type=str,required=True, help="ERA-5 Land directory")
    parser.add_argument("--arm_met_dir", type=str,required=True, help="ERA-5 Land directory")
    parser.add_argument("--arm_interp_dir",type=str,required=True, help="ERA-5 Land directory")
    parser.add_argument("--imerg_dir", type=str,required=True, help="ERA-5 Land directory")
    
    
    args, unknown = parser.parse_known_args()
    
    set_up_dfts(era_land_dir=args.era_land_dir,
                era_p=args.era_p_dir,
                era_single=args.era_surface_dir,
                arm_met=args.arm_met_dir,
                arm_interp=args.arm_interp_dir,
                imerg=args.imerg_dir).create_dfts()
    
    
            

#%%
"""
#arm_dir = '/Users/nalex2023/main/LSB_main/LSB_APP/Docker_files/projects/Manus/Datasets/twpmetC1.b1/'
arm_dir = '/Users/nalex2023/main/LSB_main/LSB_APP/Docker_files/projects/Manus/Datasets/twpinterpolatedsondeC1.c1/'

#arm_files = glob.glob(f'{arm_dir}*.cdf')

arm_files = glob.glob(f'{arm_dir}*.nc')

arm_dft = pd.DataFrame(columns=['filepath'])
arm_dft['filepath'] = arm_files

arm_dft['filename'] = arm_dft['filepath'].str.split(os.sep).str[-1]

arm_dft['datetime'] = arm_dft['filename'].str.split('.').str[2]

arm_dft['datetime'] = pd.to_datetime(arm_dft['datetime'],format='%Y%m%d')


#%%


imerg_dir = '/Users/nalex2023/main/LSB_main/LSB_APP/Docker_files/projects/Manus/Datasets/IMERG/'

imerg_files = glob.glob(f'{imerg_dir}*.nc')

imerg_dft = pd.DataFrame(columns=['filepath'])
imerg_dft['filepath'] = imerg_files

imerg_dft['filename'] = imerg_dft['filepath'].str.split(os.sep).str[-1]

imerg_dft["datetime"] = imerg_dft["filename"].str.extract(r"^([\d\-]+ [\d:\.]+)")

imerg_dft["datetime"] = pd.to_datetime(imerg_dft['datetime'],format='%Y-%m-%d %H:%M:%S.%f')
imerg_dft['datetime'] = imerg_dft['datetime'].dt.round('30min')

i

#%%
era_dir = '/Users/nalex2023/main/LSB_main/LSB_APP/Docker_files/projects/Manus/Datasets/reanalysis-era5-single-levels/'
all_files = glob.glob(f'{era_dir}*.nc')
      

era_dft = pd.DataFrame(columns=['filepath'])
era_dft['filepath'] = all_files

era_dft['filename'] = era_dft['filepath'].str.split(os.sep).str[-1]

era_dft['datetime'] = era_dft['filename'].str.split('_').str[1]
era_dft['datetime'] = pd.to_datetime(era_dft['datetime'],format='%Y-%m-%d')

era_dft['variable'] = era_dft['filename'].str.extract(r'^[^_]+_[^_]+_(.+)\.nc$')

"""


            
            
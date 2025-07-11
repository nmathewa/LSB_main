#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Dec 21 10:57:31 2024

@author: nmathewa
"""

import pandas as pd
import xarray as xr 
import glob 
import os
from windrose import WindroseAxes
#%%

in_fol = '/media/nmathewa/nma_backup/Datasets/Datasets/met_manus'

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

dft_met = create_dft(in_fol,True)


test = xr.open_dataset(dft_met.loc[0]['filepath'])















#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 26 16:57:37 2025

@author: nmathewa
"""

import pandas as pd
import glob
import os 
import numpy as np

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


sonde_adj = '/media/nmathewa/nma_backup/Datasets/Datasets/sondeadjust_manus'

sond_ori = '/media/nmathewa/nma_backup/Datasets/Datasets/sondewnpnC1_manus'


sonde_adj_dft = create_dft(sonde_adj)


sonde_ori_dft = create_dft(sond_ori)


sonde_all_dft = pd.concat([sonde_adj_dft,sonde_ori_dft])



all_days = pd.DataFrame(pd.date_range(start='2000-01-01',end='2014-12-31',freq='D'),columns=['date'])




sonde_all_dft['date'] = sonde_all_dft['datetime'].dt.date


new_count = (sonde_all_dft.groupby('date').count()['filepath']).to_frame()

new_count_sond = new_count[new_count['filepath'] >= 3]


new_count_sond['diff'] = new_count_sond.index.diff()


date_range = pd.date_range(start=new_count.index[0],
                           end=new_count.index[-1],
                           freq='D').date


new_count_sond_re = new_count_sond.reindex(date_range)





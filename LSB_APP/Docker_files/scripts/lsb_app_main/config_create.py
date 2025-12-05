#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  5 14:15:58 2025

@author: nalex2023
"""

import pandas as pd 
import os
import sys

class config_creator:
    
    def __init__(self,args):
        # get all json variables 
        #1. Project name / name 
        #2. lat 3. lon , start date (DD/MM/YYYY)
        #4. end date (DD/MM/YYYY)
        self.name = args[0]
        self.lat = args[1]
        self.lon = args[2]
        self.start = args[3]
        self.end = args[4]
        self.basepath = '/projects'
        
        
    
    def check_dft(self):
        
        if isinstance(self.name, str):
            pass
        else:
            raise ValueError
        
        if isinstance(self.lat, float):
            pass 
        else:
            raise ValueError
        
        if isinstance(self.lon, float):
            pass
        else:
            raise ValueError
        
        if isinstance(self.start, str):
            pass 
        else:
            raise ValueError
        
        return None 
    
    
    def create_dft(self):
        st_dt = pd.to_datetime(self.start,
                               format='%d/%m/%Y')
        
        et_dt = pd.to_datetime(self.start,
                               format='%d/%m/%Y')
        
        project_dft = pd.DataFrame([{
                            'Name': self.name,
                                    'Latitude': self.lat,
                                    'Longitude': self.lon,
                                    'Start Date': st_dt,
                                    'End Date': et_dt
                                    }])
        
        return project_dft
    
    
    def create_project(self,test=False):
        
        dft_project = self.create_dft()
        
        if test:
            wrd_cu = os.getcwd()
            os.mkdir(f'{wrd_cu}/{self.name}')
            dft_project.to_csv(f'{wrd_cu}/{self.name}/config.csv')
        
        if os.path.isdir(f'{self.basepath}/{self.name}'):
            print(f'Folder {self.name} already exists')
            raise FileExistsError()
        else:
            os.mkdir(f'{self.basepath}/{self.name}')
            dft_project.to_csv(f'{self.basepath}/{self.name}/config.csv',index=None)
        

#%%
                    
   

if __name__ == '__main__':
    args = sys.argv[1:]
    args[1] = float(args[1])
    args[2] = float(args[2])
    1
    config_creator(args=args).create_project()
    

        

    
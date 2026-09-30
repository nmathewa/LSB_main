#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Dec  9 16:30:24 2025

@author: nalex2023
"""

import act
import os
import argparse
import sys

class get_arm_datasets:


    def __init__(self,start_time,end_time,out_fol,dataset='twpmetC1.b1'):
        self.dataset = dataset
        self.start_time = start_time
        self.end_time = end_time
        self.out_fol = out_fol

        # Credentials come from the environment, never from source.
        # Set before running:
        #   export ARM_USERNAME="your_arm_username"
        #   export ARM_TOKEN="your_arm_token"
        self.username = os.environ.get("ARM_USERNAME")
        self.token = os.environ.get("ARM_TOKEN")

        if not self.username or not self.token:
            sys.stderr.write(
                "ERROR: ARM_USERNAME and ARM_TOKEN must be set in the environment.\n"
                "Get them from https://adc.arm.gov/armlive/ then:\n"
                "  export ARM_USERNAME='...'\n"
                "  export ARM_TOKEN='...'\n"
            )
            sys.exit(1)
    
    def check_path(self):
        check = os.path.exists(f'{self.out_fol}{self.dataset}')
        
        return check 
            
    
    
    def act_download(self):
        
        check_con = self.check_path()
        
        if not check_con:
            os.chdir(f'{self.out_fol}')
            act.discovery.download_arm_data( username=self.username,
                                            token=self.token,datastream=self.dataset,
                                            startdate=self.start_time, enddate=self.end_time
            )
        
        else:
            print('Folder found skipping')
            

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ARM ACT data downloader")
    
    # REQUIRED: --var now accepts multiple arguments (nargs='+')
    parser.add_argument("--datastream", type=str, default='twpmetC1.b1',required=True, help="Datastrem eg:twpmetC1.b1")
    parser.add_argument("--start_date", type=str, required=False, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end_date", type=str, required=False, help="End date (YYYY-MM-DD)")
    parser.add_argument("--download_fol", type=str, required=False, help="Output directory path")
    args, unknown = parser.parse_known_args()
    
    get_arm_datasets(dataset=args.datastream,
                     start_time=args.start_date,
                     end_time=args.end_date, 
                     out_fol=args.download_fol).act_download()
    
    

"""
act.discovery.download_arm_data( username=os.environ["ARM_USERNAME"],
                                token=os.environ["ARM_TOKEN"],datastream="twpmetC1.b1", startdate="2000-01-01", enddate="2000-01-20"
)
"""
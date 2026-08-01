import sys
import os
import pandas as pd
import json

class config_creator:
    
    def __init__(self, args):
        self.name = args[0]
        self.lat = float(args[1])
        self.lon = float(args[2])
        self.start = args[3]
        self.end = args[4]
        self.basepath = '/projects' 
    
    def create_dft(self):
        st_dt = pd.to_datetime(self.start, format='%d/%m/%Y')
        et_dt = pd.to_datetime(self.end, format='%d/%m/%Y') 
        
        # Create DataFrame
        # We use str() here to ensure dates are JSON-compatible strings immediately
        project_dft = pd.DataFrame([{
            'Name': self.name,
            'Latitude': self.lat,
            'Longitude': self.lon,
            'Start Date': str(st_dt), 
            'End Date': str(et_dt)    
        }])
        
        return project_dft
    
    def create_project(self, test=False):
        """
        Creates the folders and returns a JSON-ready Dictionary.
        """
        dft_project = self.create_dft()
        proj_dir = f'{self.basepath}/{self.name}'

        # 1. Folder Creation Logic
        if os.path.isdir(proj_dir):
            sys.stderr.write(f"Folder {self.name} already exists skipping\n")
        else:
            try:
                os.makedirs(f'{proj_dir}/Datasets', exist_ok=True)
                os.makedirs(f'{proj_dir}/Reports', exist_ok=True)
                os.makedirs(f'{proj_dir}/Plots', exist_ok=True)
                os.makedirs(f'{proj_dir}/extras', exist_ok=True)
                dft_project.to_csv(f'{proj_dir}/config.csv', index=None)
                sys.stderr.write(f"Created config at {proj_dir}/config.csv\n")
            except Exception as e:
                sys.stderr.write(f"Error creating directories: {e}\n")

        # 2. CONVERSION LOGIC (Moved Inside Function)
        # Convert the DataFrame to a single Dictionary object
        project_dict = dft_project.to_dict(orient='records')[0]
        
        return project_dict

if __name__ == "__main__":
    # 1. Get arguments
    if len(sys.argv) < 6:
        input_args = ["Manus", "-2.0", "147.0", "01/10/2011", "01/10/2011"]
    else:
        input_args = sys.argv[1:]

    # 2. Run Class
    creator = config_creator(input_args)
    
    # Now this returns a clean dictionary directly
    result = creator.create_project()

    # 3. Print JSON to stdout
    print(json.dumps(result))
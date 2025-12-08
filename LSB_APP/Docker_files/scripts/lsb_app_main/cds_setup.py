#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Dec  5 15:23:43 2025

@author: nalex2023
"""

import os
from pathlib import Path

def create_cdsapirc(uid: str, api_key: str):
    """
    Create or replace ~/.cdsapirc with provided credentials.
    """
    content = f"""url: https://cds.climate.copernicus.eu/api/v2
key: {uid}:{api_key}
"""
    cdsapirc_path = Path.home() / ".cdsapirc"

    try:
        with open(cdsapirc_path, "w") as f:
            f.write(content)
        print(f"Successfully wrote CDS API credentials to {cdsapirc_path}")
    except Exception as e:
        print(f"Error writing to {cdsapirc_path}: {e}")

if __name__ == "__main__":
    # Get UID and API key from environment variables (set via n8n)
    uid = os.environ.get("CDS_UID")
    api_key = os.environ.get("CDS_KEY")

    if not uid or not api_key:
        raise ValueError("CDS_UID and CDS_KEY environment variables must be set!")

    create_cdsapirc(uid, api_key)
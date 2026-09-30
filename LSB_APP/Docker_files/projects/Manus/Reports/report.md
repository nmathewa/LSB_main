---
title: "Preliminary Data Availability Report: ERA5 and ARM Datasets"
author: Manus Research Team
date: "2017-02-20"
subject: "Markdown"
keywords: [ERA5, ARM, IMERG, Data Report, Meteorological Datasets]
subtitle: "Dataset insights for reanalysis, observational, and satellite-based meteorological data"
lang: "en"
titlepage: true
titlepage-text-color: "FFFFFF"
titlepage-rule-color: "360049"
titlepage-rule-height: 0
titlepage-background: "/projects/Manus/Reports/background.pdf"
---

# Preliminary Data Availability Report: Reanalysis and Observational Datasets

## Introduction

This document provides an organized summary of available meteorological datasets hosted under the Manus project directory. Each dataset is reviewed for its file format, temporal coverage, variable content, units, and file naming conventions. The datasets included are derived from reanalysis models (ERA5), ground-based observational sources (ARM), and remote sensing (IMERG).

## Study Region

![Figure 1: Regional domain for all included datasets](/projects/Manus/Plots/Figure1.png){ width=40% }

---

## ERA5 Land Dataset

### Dataset Overview

- **Path:** `/projects/Manus/Datasets/reanalysis-era5-land/`
- **Format:** NetCDF (.nc)
- **Total Files:** 244
- **Temporal Range:** 2011-10-01 to 2012-05-01 (discrete dates)
- **Variables:**
  - `2m_temperature`: Near-surface air temperature
  - `10m_v_component_of_wind`: Vertical (north-south) wind component

### Variables and Units

| Variable                | Description                               | Units   |
|-------------------------|-------------------------------------------|---------|
| 2m_temperature          | Air temperature at 2m above surface       | Kelvin  |
| 10m_v_component_of_wind | Vertical wind component at 10m above ground | m/s   |

### File Naming Convention

- Files follow: `reanalysis-era5-land_YYYY-MM-DD_<variable>.nc`
- Most files contain `2m_temperature`; a subset has `10m_v_component_of_wind`.

### Summary

| Attribute        | Details                                         |
|------------------|-------------------------------------------------|
| Dataset Name     | ERA5 Land                                       |
| Number of Files  | 244                                             |
| File Format      | NetCDF (.nc)                                   |
| Date Range       | 2011-10-01 to 2012-05-01                        |
| Variables        | 2m_temperature, 10m_v_component_of_wind        |
| Units            | Kelvin (K) and m/s                             |
| File Path        | `/projects/Manus/Datasets/reanalysis-era5-land/` |

### Conclusion

ERA5 Land provides near-surface meteorological data across multiple dates. The dataset prioritizes temperature fields and is well-suited for land-atmosphere interaction studies.

---

## ERA5 Pressure Levels Dataset

### Dataset Overview

- **Path:** `/projects/Manus/Datasets/reanalysis-era5-pressure-levels/`
- **Format:** NetCDF (.nc)
- **Total Files:** 59
- **Date Range:** 2011-10-01 to 2011-11-28
- **Primary Variable:** `temperature`

### Variables and Units

| Variable    | Description                     | Units   |
|-------------|----------------------------------|---------|
| temperature | Air temperature at pressure levels | Kelvin  |

### File Organization

- Files are named as: `reanalysis-era5-pressure-levels_YYYY-MM-DD_temperature.nc`
- Each file includes temperature profiles across pressure levels for one date.

### Summary

| Attribute      | Details                                      |
|----------------|----------------------------------------------|
| Dataset Name   | ERA5 Pressure Levels                         |
| File Count     | 59                                           |
| Format         | NetCDF (.nc)                                 |
| Temporal Scope | 2011-10-01 to 2011-11-28                     |
| Variable       | Temperature (K)                              |
| Data Location  | `/projects/Manus/Datasets/reanalysis-era5-pressure-levels/` |

### Conclusion

The ERA5 Pressure Levels dataset is comprised of high-resolution temperature profiles ideal for upper atmosphere thermodynamic studies over the specified period.

---

## ERA5 Single Levels Dataset

### Dataset Overview

- **Path:** `/projects/Manus/Datasets/reanalysis-era5-single-levels/`
- **Format:** NetCDF (.nc)
- **Files Available:** 642
- **Date Coverage:** 2011-10-01 to 2012-05-01

### Variables and Units

| Variable                | Description                                | Units   |
|-------------------------|--------------------------------------------|---------|
| 2m_temperature          | Near-surface air temperature               | Kelvin  |
| 10m_u_component_of_wind | East-west wind component                   | m/s     |
| 10m_v_component_of_wind | North-south wind component                 | m/s     |

### File Format

- File pattern: `reanalysis-era5-single-levels_YYYY-MM-DD_<variable>.nc`
- Each file stores one variable per date.

### Summary

| Attribute        | Details                                        |
|------------------|------------------------------------------------|
| Dataset Name     | ERA5 Single Levels                             |
| File Count       | 642                                            |
| File Format      | NetCDF (.nc)                                   |
| Temporal Range   | 2011-10-01 to 2012-05-01                        |
| Variables        | 2m_temperature, 10m_u_component_of_wind, 10m_v_component_of_wind |
| Units            | Kelvin, m/s                                    |
| File Path        | `/projects/Manus/Datasets/reanalysis-era5-single-levels/` |

### Conclusion

ERA5 Single Levels provides a detailed series of surface parameters, particularly relevant for boundary layer meteorology and surface energy balance assessments.

---

## ARM Meteorological Observations (MET)

### Dataset Overview

- **Path:** `/projects/Manus/Datasets/twpmetC1.b1/`
- **Format:** NetCDF (.cdf)
- **Total Files:** 215
- **Coverage:** 2011-10-01 to 2012-05-01
- **Filename Structure:** `twpmetC1.b1.YYYYMMDD.HHMMSS.cdf`

### Data Characteristics

- Ground-based point measurements from ARM TWP C1 site.
- Contains key meteorological metrics such as:
  - Temperature (°C)
  - Humidity (%)
  - Wind (m/s)
  - Pressure (Pa or mb)

### Summary

| Attribute        | Details                                    |
|------------------|--------------------------------------------|
| Dataset Name     | ARM MET (TWP C1.b1)                        |
| Total Files      | 215                                        |
| Format           | NetCDF (.cdf)                              |
| Time Range       | 2011-10-01 to 2012-05-01                    |
| Variables        | Standard meteorological (temp, RH, wind, etc.) |
| File Structure   | `twpmetC1.b1.YYYYMMDD.HHMMSS.cdf`           |
| Units (Expected) | °C, %, m/s, Pa                             |

### Conclusion

The ARM MET dataset provides in-situ atmospheric observations ideal for validating model-based results from ERA5 or satellite precipitation estimates.

---

## ARM Interpolated Sonde Dataset

### Dataset Overview

- **Path:** `/projects/Manus/Datasets/twpinterpolatedsondeC1.c1/`
- **Format:** NetCDF (.nc)
- **Files:** 213
- **Time Coverage:** 2011-10-01 to 2012-04-30

### File Naming Convention

- Filename: `twpinterpolatedsondeC1.c1.YYYYMMDD.000030.nc`

### Dataset Description

- Derived vertical profiles from radiosonde observations.
- Likely includes variables such as:
  - Temperature (°C or K)
  - Relative Humidity (%)
  - Wind speed/direction (m/s, degrees)
  - Atmospheric pressure (hPa)

### Summary Table

| Attribute      | Description                                   |
|----------------|-----------------------------------------------|
| Dataset Name   | ARM Interpolated Sonde (C1)                   |
| File Count     | 213                                           |
| Format         | NetCDF (.nc)                                  |
| Period         | 2011-10-01 to 2012-04-30                       |
| Variables      | Vertical profiles (temp, RH, pressure, winds) |
| Units          | °C/K, %, m/s, hPa                             |
| Dataset Path   | `/projects/Manus/Datasets/twpinterpolatedsondeC1.c1/` |

### Conclusion

This high-resolution dataset supports vertical atmospheric structure examination and is essential for evaluating atmospheric stability and thermodynamic evolution.

---

## IMERG Satellite-Based Precipitation Dataset

### Dataset Overview

- **Path:** `/projects/Manus/Datasets/IMERG/`
- **Format:** NetCDF (.nc)
- **File Count:** 3,172
- **Date Range:** Roughly October to December 2011
- **File Naming Pattern:** `YYYY-MM-DD hh:mm:ss.ssssss_IMEG07.nc`

### Characteristics

- Half-hourly global precipitation estimates.
- Based on Integrated Multi-satellitE Retrievals for GPM (IMERG).
- Variable of interest: **Precipitation rate**, typically in **mm/hr**.

### Summary Table

| Attribute             | Details                                   |
|------------------------|-------------------------------------------|
| Dataset Name          | IMERG (NASA GPM Precipitation)            |
| Number of Records     | 3,172                                     |
| Format                | NetCDF (.nc)                             |
| Temporal Resolution   | 30 min intervals                          |
| Time Period           | October 2011 – December 2011 (approx.)    |
| Primary Variable      | Precipitation rate                        |
| Units                 | mm/hr                                     |
| Storage Path          | `/projects/Manus/Datasets/IMERG/`         |

### Conclusion

IMERG provides high-frequency global precipitation data, suitable for weather event tracking and hydrological modeling for the studied region and timeframe.

---

## Final Notes

This report was developed strictly from available metadata and standard dataset documentation from ECMWF, ARM, and NASA. No unauthorized assumptions were made regarding data content or accuracy. Figures and data distributions should be explored further in secondary analysis to derive climatological or model-evaluation insights.

---
# algo_main

`lsb_detection.py` is the v3 land-sea breeze detector, extracted from `LSB_detectionv3/detection_main.ipynb`.

```python
import lsb_detection as L

sun = L.load_sun_times("LSB_detectionv2/Datasets/sunrise_sunset_twp.csv", "2011-11", "2012-06")
ds = L.attach_sun_times(ds, sun)          # ds: 30-min xarray Dataset in local time
result = L.detect(ds, "2011-11-01", "2012-06-01")
result[result["sea_breeze"]].to_csv("sea_breeze.csv")
```

## Known inconsistencies carried over from the notebook

Left unchanged so results match the notebook; worth a decision before the next release.

- `sea_breeze` combines filters 2, 3, 4, 6 and 7. Filters 8 (pressure) and 9 (tide) are computed but not used.
- `filter4_prev_cond` reads `wspd_vec_mean` as the "previous wind direction"; `wdir_vec_mean` may be intended.
- The tide term is `(p04 - p16)` in the climatology cell (`tide_reference`) but `(p04 + p16)` in `filter9_tide`, and `atide_gen` is hard-coded to 2017.4575.
- The notebook's final cell masked with the string column `filter6_wsmax`; the module uses the boolean `filter_6`.

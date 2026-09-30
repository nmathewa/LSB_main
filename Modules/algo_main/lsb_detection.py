"""Land-sea breeze (LSB) detection, v3.

Extracted from ``LSB_detectionv3/detection_main.ipynb``. The filter logic is
unchanged; paths and the time window are now arguments.

Expected input: an ``xarray.Dataset`` on a 30-minute ``time`` axis in local time
with ``wspd_arith_mean``, ``wdir_vec_mean``, ``wspd_vec_mean`` and
``atmos_pressure`` (hPa), plus per-``day`` ``sunrise_local`` / ``sunset_local``
(see :func:`attach_sun_times`).

Filters
    2  wind-speed onset: 30-min speed jump > 0.5 within sunrise+1h..+7h
    3  wind direction within 0-180 deg in the same window
    4  the step before onset shows no strong prior acceleration (< 1.5) or an offshore direction
    6  cessation: post-sunset max speed < 2.5 (sunset-1h..+5h)
    7  synoptic screen: max speed in the onset window < 6
    8  pressure between onset and cessation does not rise more than 1 hPa above its minimum
    9  semidiurnal tide anomaly <= -0.5 relative to ``atide_gen``
"""
import numpy as np
import pandas as pd
import xarray as xr

FREQ = "30min"
ONSET_WINDOW_H = (1, 7)     # hours after sunrise
CESS_WINDOW_H = (-1, 5)     # hours around sunset


def load_sun_times(csv_path, start, end, utc_offset_h=10):
    """Read a sunrise/sunset CSV (UTC columns) and add local-time columns."""
    sun = pd.read_csv(csv_path).map(pd.to_datetime)
    sun["sunrise_local"] = sun["sunrise(UTC)"] + pd.Timedelta(hours=utc_offset_h)
    sun["sunset_local"] = sun["sunset(UTC)"] + pd.Timedelta(hours=utc_offset_h)
    return sun[(sun["date"] >= start) & (sun["date"] <= end)]


def attach_sun_times(ds, sun):
    """Add ``sunrise_local`` / ``sunset_local`` on a ``day`` dimension."""
    coords = {"day": sun["date"]}
    ds["sunrise_local"] = xr.DataArray(sun["sunrise_local"].values, dims="day", coords=coords)
    ds["sunset_local"] = xr.DataArray(sun["sunset_local"].values, dims="day", coords=coords)
    return ds


def _window(ds, anchor_var, day, hours):
    """Data in ``anchor + hours[0] .. anchor + hours[1]``; ``None`` if unavailable."""
    try:
        anchor = ds[anchor_var].sel(day=str(pd.to_datetime(day).date())).values
    except KeyError:
        return None
    times = pd.date_range(anchor + pd.Timedelta(hours=hours[0]),
                          anchor + pd.Timedelta(hours=hours[1]), freq=FREQ).round(FREQ)
    win = ds.sel(time=times)
    return win if win.time.size else None


def _first_time(series):
    return series.time[0].dt.time.astype(str).values if series.size > 0 else np.nan


def filter2_ws_onset(ds, day, min_jump=0.5):
    win = _window(ds, "sunrise_local", day, ONSET_WINDOW_H)
    if win is None:
        return np.nan
    ws = win["wspd_arith_mean"]
    diff = ws.diff(dim="time").reindex(time=win["time"])
    return _first_time(ws[diff > min_jump])


def filter3_wdir(ds, day, min_wd=0, max_wd=180):
    win = _window(ds, "sunrise_local", day, ONSET_WINDOW_H)
    if win is None:
        return np.nan
    wd = win["wdir_vec_mean"]
    return _first_time(wd[(wd >= min_wd) & (wd <= max_wd)])


def filter4_prev_cond(ds, row, max_prev_jump=1.5, offshore_above=181):
    """``row`` needs ``datetime`` (Timestamp) and ``filter2_wspd`` (onset time string)."""
    onset = row["datetime"].strftime("%Y-%m-%d") + " " + row["filter2_wspd"]
    prev = pd.to_datetime(onset) - pd.Timedelta(minutes=30)
    jump_prev = ds["wspd_arith_mean"].diff(dim="time").sel(time=prev)
    wd_prev = ds["wspd_vec_mean"].sel(time=prev)
    return bool((jump_prev < max_prev_jump) or (wd_prev > offshore_above))


def filter6_ws_cess(ds, day, max_speed=2.5):
    win = _window(ds, "sunset_local", day, CESS_WINDOW_H)
    if win is None:
        return np.nan
    ws = win["wspd_arith_mean"]
    return ws.idxmax().dt.time.astype(str).values if ws.max() < max_speed else np.nan


def filter7_synop(ds, day, max_speed=6):
    win = _window(ds, "sunrise_local", day, ONSET_WINDOW_H)
    if win is None:
        return np.nan
    return bool(win["wspd_arith_mean"].max() < max_speed)


def filter8_pres(ds, row, max_rise=1):
    """``row.name`` is the day; needs ``filter2_wspd`` and ``filter6_wsmax``."""
    onset, cess = row["filter2_wspd"], row["filter6_wsmax"]
    if pd.isna(onset) or pd.isna(cess):
        return np.nan
    date = pd.to_datetime(row.name).strftime("%Y-%m-%d")
    t_on = pd.to_datetime(f"{date} {onset}")
    t_ce = pd.to_datetime(f"{date} {cess}")
    p = ds["atmos_pressure"]
    return (p.sel(time=t_ce).item() - p.sel(time=slice(t_on, t_ce)).min().item()) <= max_rise


def tide_reference(ds):
    """Climatological ``atide_gen`` from the hour-of-day mean pressure.

    NOTE: the notebook computes this as (p10 + p22) - (p04 - p16) but the daily
    value in :func:`filter9_tide` as (p10 + p22) - (p04 + p16); ``atide_gen`` was
    then hard-coded to 2017.4575. Kept as-is; see README.
    """
    h = ds["atmos_pressure"].resample(time="1h").mean().groupby("time.hour").mean()
    return float(((h.sel(hour=10) + h.sel(hour=22)) - (h.sel(hour=4) - h.sel(hour=16))).values)


def filter9_tide(ds, day, atide_gen=2017.4575, threshold=-0.5):
    times = pd.date_range(start=str(pd.to_datetime(day).date()), periods=48, freq=FREQ)
    hourly = ds["atmos_pressure"].sel(time=times).resample(time="1h").mean()

    def at(h):
        return hourly.where(hourly.time.dt.hour == h, drop=True).squeeze().item()

    atide = (at(10) + at(22)) - (at(4) + at(16))
    return (atide - atide_gen) <= threshold


def detect(ds, start, end, atide_gen=2017.4575):
    """Run all filters day by day; returns a DataFrame indexed by day.

    ``sea_breeze`` combines filters 2, 3, 4, 6 and 7 (as in the notebook).
    Filters 8 and 9 are reported but not part of that flag.
    """
    days = pd.date_range(start=start, end=end, freq="1D").to_series()
    out = pd.DataFrame({"datetime": days})
    out["filter2_wspd"] = days.apply(lambda d: filter2_ws_onset(ds, d))
    out["filter2"] = out["filter2_wspd"].notna()
    out["filter3_wdir"] = days.apply(lambda d: filter3_wdir(ds, d))
    out["filter3_result"] = out["filter3_wdir"].notna()

    has_onset = out[out["filter2"]]
    out["filter4_prev_wind"] = (has_onset.apply(lambda r: filter4_prev_cond(ds, r), axis=1)
                                .reindex(out.index).fillna(False).astype(bool))

    out["filter6_wsmax"] = days.apply(lambda d: filter6_ws_cess(ds, d))
    out["filter_6"] = out["filter6_wsmax"].notna()
    out["filter7_synop"] = days.apply(lambda d: filter7_synop(ds, d))
    out["filter8_pres"] = out.apply(lambda r: filter8_pres(ds, r), axis=1)
    out["filter9_tide"] = days.apply(lambda d: filter9_tide(ds, d, atide_gen))

    out["sea_breeze"] = (out["filter2"] & out["filter3_result"] & out["filter4_prev_wind"]
                         & out["filter_6"] & out["filter7_synop"].fillna(False).astype(bool))
    return out

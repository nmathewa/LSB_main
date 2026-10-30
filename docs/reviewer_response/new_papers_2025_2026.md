# 2025–2026 search for studies comparable to Sokolov 2020, Roy 2022 and Krishnamoorthy 2023

Search date: Oct 2026, web search only (search snippets, no full texts). Items marked ✔ were confirmed by a second, independent search. Everything else is **not confirmed** until the PDF is checked.

## A. ML on observations

| # | Item | Status | Why it matters | Action |
|---|---|---|---|---|
| A1 | Krishnamoorthy et al., "Machine Learning-Based Short-Term Forecasting of Wind Speed, Turbulence Intensity, and Sea Breeze for Offshore Wind Farm Applications", *Clean Energy* (OUP), advance article, doi:10.1093/ce/zkag059 | ✔ Confirmed (Oct 2026): accepted manuscript on Oxford Academic, authors listed as Krishnamoorthy A, Sharma D, Goyal R; final year/issue not confirmed | Possibly the peer-reviewed version of SSRN 4620501. If so, cite it instead of the preprint and re-check the sea-breeze evaluation (numbers may differ) | **Get PDF** (https://academic.oup.com/ce/advance-article-pdf/doi/10.1093/ce/zkag059/70983941/zkag059.pdf) |
| A2 | "UNet-based methods for identifying Chicago lake breeze fronts from NEXRAD", AGU Fall Meeting 2025, A43R-2375 | Conference abstract | ResNet/UNet on more than 7,000 radar scans. Labels: hand-drawn fronts on Micronet-flagged days. P/R/F1 > 0.96 (preliminary). One site | Same status as Bedoya-Valestt (abstract). At most a one-line outlook mention |
| A3 | Hong Kong Observatory, probabilistic forecast of runway headwind changes (sea-breeze effect), ICAO APAC MET/R WG/14 slides (2025) | Grey literature | Operational ML use at an airport. No methods or validation given | Optional mention as operational use; not evidence |

## B. ML on model output

- **Naegele et al. (2025), AIES 4(2):** SOMs of HRRR coastal wind regimes. doi:10.1175/AIES-D-24-0023.1. Unsupervised; a sea-breeze regime is not confirmed.
- **Zhang et al. (2026), AMT 19, 5071:** bi-clustered ML correction of WRF offshore wind forecasts, accounting for sea–land breeze and stability. Wind forecast correction, not sea-breeze detection. https://amt.copernicus.org/articles/19/5071/2026/

## C. Non-ML and near misses

1. ✔ **Ssemujju, Maranan, Fink & Ageet (2026).** A novel algorithm for detecting lake-breeze fronts at Lake Victoria using station observations. *JAMC* 65(6), 941–960. doi:10.1175/JAMC-D-25-0253.1. Rule-based; 45 AWS, 15-min data, 2017–22; tested against manually identified days. Skill scores (ACC ≈ 0.76, POD > 0.7, POFA ≈ 0.2) come from a snippet and are not confirmed. **Useful for the evaluation-protocol paragraph:** a multi-station detector evaluated against an independent reference, with POD and false-alarm rate reported. Companion paper in *Atmos. Sci. Lett.* (2026) applies it to ECMWF IFS.
2. **Shen et al. (2026).** Progress in the study of sea land breeze: identification, observation, and interacting factors. *Sci. China Earth Sci.* 69(3), 974–991. doi:10.1007/s11430-025-1741-5. A recent review on the same topic. **Check overlap with your review and consider citing it.**
3. Niubó-Pallàs et al. (2025). EMS2025-407. Catalonia, 98 stations; filters plus k-means regionalisation (abstract).
4. Huang et al. (2025). *GRL*, doi:10.1029/2024GL112480. China sea–land breeze frequency from ERA5 (rule-based).
5. Zhou et al. (2026). *JGR-Atmos*, doi:10.1029/2025JD045967. Qingdao sea–land breeze structure.
6. *AMT* 18, 1269 (2025). ML analysis of Mid-Atlantic nocturnal low-level jets from radar wind profilers (a methods analogue).
7. Patent CN122087587A, ML sea-wind front recognition (patent, unverified).
8. Haruna et al. (2026). *Sensors* 26, 448. Livorno port wind nowcasting with RF, XGBoost, LSTM, TCN and Transformer; any sea-breeze analysis is not confirmed.
9. Air-quality ML using sea breeze as a predictor (Pearl River Delta ozone; India east coast). Not detection.
10. Warn-on-Forecast evaluation for coastal breezes (WAF-D-25-0090.1). NWP, not ML.
11. **Unverified lead:** RNN/LSTM/Conv1D-LSTM for sea-breeze onset time near Kalpakkam, India (ResearchGate listing). Title and year not confirmed.

## Bottom line

- No new peer-reviewed study comparable to the three was found for 2025–2026, apart from the possible journal version of Krishnamoorthy et al. (A1).
- No ML study evaluates at more than one site.
- This supports stating the thin evidence base as a gap in the field.

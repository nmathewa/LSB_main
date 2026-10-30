# Paper-by-paper notes for Section 3.4.1

Values marked *derived* are computed by us from the paper's published tables and figures, not reported by the authors.

---

## 1. Sokolov, Dmitriev, Gengembre & Delbarre (2020)

*Automated classification of regional meteorological events in a coastal area using in situ measurements.* J. Atmos. Oceanic Technol., 37, 723–739. https://doi.org/10.1175/JTECH-D-19-0120.1 (peer-reviewed)

### Design

| Item | What the paper did | Where |
|---|---|---|
| Site | Dunkirk harbour, English Channel coast (one site) | Sec. 2, Fig. 1 |
| Period | July 2013 – Aug 2014 (442 days) | Sec. 2, Table 3 |
| Predictors | Ultrasonic anemometer (Metek USA-1) + weather station, 15-min resolution: wind, u/v, u*, σw, heat and momentum flux, T, RH, p, solar radiation, rain (13 variables; plus 2-h means in the METEO+Avg2h experiment) | Table 1, Sec. 6b |
| Labels | **Manual expert classification** (Gengembre 2018 thesis) using anemometer, weather station, Doppler lidar wind profiles and a clear-sky radiation model. Sea breeze: anticyclonic conditions, sea-wind sector NW–NE, low cloud, rapid transitions in T, RH and wind, confirmed with lidar profiles. Lidar was used **for labelling, not as a predictor** | Sec. 3a |
| Classes | 4: Others, Breeze, Fog, High-wind event (HWE) | Sec. 6 |
| Unit classified | **15-min sample** (not day) | Table 3 |
| Class balance | Samples: Others 40,556; Breeze 1,127; Fog 564; HWE 184 (total 42,431) → breeze = **2.7 % of samples**. Days: breeze 43 of 442 (9.7 %) | Table 3 |
| Rebalancing | Others class subsampled to **1,000 samples**, resampled 49× (KNN, SVM, QDA) or 11× (PRW). Confusion matrices summed over 100 resamplings. Evaluation base rate for breeze ≈ **0.39**, not 0.027 | Sec. 6a, Fig. 4 caption |
| Validation | **10-fold block cross-validation** (explicitly to avoid dependence between neighbouring 15-min samples). Confidence intervals by resampling. All within one site and one campaign | Sec. 5 |
| Metrics | TOE = mean omission error over the 4 classes (= 1 − mean per-class detection rate); TCE = mean commission error; κ (class-averaged definition, Table 2) | Table 2 |

### What "up to 80 %" means

It is **1 − TOE**: the mean per-class detection rate over the four classes, on the rebalanced sample. Best values: KNN 0.84, QDA 0.84 (possible feature sets, Table 5); about 0.77–0.78 with two features. The conclusion says "70 %–80 % of events". It is **not overall accuracy and not sea-breeze-specific**.

### Within-study result (valid to report)

- Four classifiers on the same data with block CV and confidence intervals (Table 5):
  - KNN: TOE 0.159, κ 0.72
  - QDA: TOE 0.161, κ 0.72
  - SVM: TOE 0.232, κ 0.70
  - PRW: TOE 0.249, κ 0.58
- **KNN and QDA are best and not significantly different from each other.** Both beat SVM and PRW.
- Adding 2-h averaged predictors improves KNN/QDA slightly, but not significantly (Table 7).
- The most informative predictors are horizontal wind speed and humidity, then wind direction / v-component and solar radiation (Table 4).

### Sea-breeze-specific scores (*derived* from Fig. 4 confusion matrices)

| | KNN (Fig. 4) | QDA (Fig. 4) |
|---|---|---|
| Hit rate H (breeze PA) | 0.80 | 0.91 |
| False-alarm rate on Others | 0.40 | 0.18 |
| False-alarm rate on Fog | 0.004 | 0.16 |
| **PSS**, non-breeze mix as observed (Table 3) | **0.41** | **0.74** |
| PSS, rebalanced evaluation mix | 0.57 | 0.76 |
| Precision on the rebalanced sample (paper's UA) | 0.69 | 0.79 |
| **Precision at observed class frequencies** (p = 0.027) | **≈ 0.05** | **≈ 0.12** |

Method:
- F is re-weighted by the observed shares of Others, Fog and HWE among non-breeze samples (Table 3).
- Precision = H·p / (H·p + F·(1 − p)).
- PSS changes between the two mixes only because the non-breeze classes have different false-alarm rates and their proportions differ. Precision changes mainly because of the base rate.

Interpretation: the classifiers separate breeze from fog and high wind well. The weakness is breeze vs. "Others": KNN flags 40 % of Others samples as breeze. At the real class frequencies, most 15-min samples flagged as breeze would not be breeze.

### Validation level

Internal only. Block CV within a single 13-month campaign at one site. No independent year, no other site.

### Internal inconsistencies to be aware of

1. **Sea-breeze count:** the text gives 36 sea-breeze events (Sec. 3), Table 3 gives 43 breeze days.
2. **Fig. 4 QDA matrix:** the caption says HorWind + Hum. The KNN matrix matches Table 5's two-feature values (TOE 0.236 vs 0.234). The QDA matrix matches Table 5's **possible-set** values (TOE 0.159 vs 0.161; TCE 0.172 vs 0.175), not the two-feature ones (0.221, 0.267). So KNN vs. QDA in Fig. 4 is probably not like-for-like. Use Table 5 for the classifier comparison.
3. **Fig. 7 row totals:** Others 112,700; Breeze 56,400; Fog ~100,050. These do not match the class sizes used in Fig. 4 (Others ~100,050; Breeze 112,700; Fog 56,400) or Table 3. The row labels may be permuted. Do not derive values from Fig. 7.

### Non-comparability with the other studies

- Multi-class vs. binary.
- **15-min samples vs. days.**
- Rebalanced evaluation sample.
- Expert labels using lidar.
- Block CV at one site.

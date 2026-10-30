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

---

## 2. Roy, Sentchev, Fourmentin & Augustin (2022)

*Machine learning and deterministic methods for detection meteorological phenomena from ground measurements: Application for low-level jet and sea-breeze identification in northern France.* Atmosphere, 13, 1873. https://doi.org/10.3390/atmos13111873 (peer-reviewed)

### Design

| Item | What the paper did | Where |
|---|---|---|
| Site | Dunkerque (one site) | Sec. 2, Fig. 1 |
| Data | 20 Hz sonic anemometer, 15-min averages, 2018–2019. Separate 86-day IOP (23 Jul – 16 Oct 2021) with **another** sonic anemometer at the coastline (15 m AGL) and WindCube lidars (WLS7, WLS100) | Sec. 2, 3.3 |
| Methods | (i) **SCSBC**: deterministic filters (no extreme event; no > 90° direction change in < 1 h; sign change of the normalised sea-breeze component SBC from negative to positive between 08:00 and 11:00 UTC; temperature-slope check). (ii) **RNN (LSTM, 96 hidden units)**: input = daily SBC time series (96 × 15-min values) | Sec. 2.2, Figs. 2–3 |
| Labels for RNN training and testing | **SCSBC output** ("categorical SBC data from 2018"; test against "actual data of SBC from 2019"). The RNN learns to reproduce the deterministic algorithm | Sec. 3.1 |
| Unit classified | **Day** | Sec. 2.2 |
| Season | **April–September only** (182 days per year) | Sec. 2.2 |
| Train / test | **Train 2018, test 2019** (held-out year), same site and sensor | Sec. 2.2, 3.1 |
| SB days (SCSBC) | 67 (2018), 78 (2019), all in Apr–Sep | Abstract, Fig. 6 |
| Independent check | 2021 IOP: lidar-based SB identification (land→sea direction shift in < 1 h and/or a gravity current from NW–NE). **25 SB events observed by lidar; RNN detected 22 → "88 %"**. Compared by monthly counts (Jul 2/2, Aug 8/8, Sep 10/13, Oct 2/2); false alarms are not reported | Sec. 3.3 |

### What the reported numbers are

- **95 % accuracy, 98 % sensitivity, 91 % specificity**: RNN vs. **SCSBC labels**, 2019, Apr–Sep (Fig. 7c: 101 / 2 / 7 / 72). This measures how well the RNN **emulates the rule-based SCSBC algorithm**, not agreement with an independent truth.
- **88 %**: share of lidar-identified SB events (25) that the RNN also flagged (22) in the 2021 IOP. This is the only test against an independent reference.

### Which class is sea breeze in Fig. 7c?

As printed, true class 1 has 103 days and predicted class 1 has 108. But SCSBC found **78** SB days in 2019 (abstract, Fig. 6b), and the RNN's 2019 bars in Fig. 6c sum to about 75. These match class **2** (79 true, 74 predicted). So class 1 is probably **non-SB**. In that case:
- SB sensitivity is **91 %**.
- The "98 %" is the detection rate for **non-SB** days.

PSS and κ are the same under either reading; precision is not.

### Derived scores (*derived*, 2019 test vs. SCSBC)

| | Class 2 = SB (likely) | Class 1 = SB (as printed) |
|---|---|---|
| Base rate (Apr–Sep 2019) | 0.43 | 0.57 |
| Hit rate H | 0.91 | 0.98 |
| False-alarm rate F | 0.02 | 0.09 |
| **PSS** | **0.89** | **0.89** |
| κ | 0.90 | 0.90 |
| **Precision** | **0.97** | 0.94 |

2021 IOP vs. lidar: H ≤ 0.88 (22/25, matched by monthly counts, not day by day). F is not reported, so PSS and precision cannot be derived; PSS ≤ 0.88 at best.

### Validation level

- 2019 test: **held-out year**, same sensor and site, reference = the rule-based algorithm the RNN was trained to emulate.
- 2021 IOP: **different year, different anemometer position, independent instrument (lidar)**. Same coastline. Hit rate only, by counts.
- This is the only one of the cited studies with any independent-reference test.

### Non-comparability with the other studies

- Day vs. 15-min samples (Sokolov).
- Warm season only (Apr–Sep).
- Reference = rule-based algorithm (main test) or lidar criteria (IOP), not expert labels.
- Binary task.
- Same Dunkirk area as Sokolov et al. (2020) and the same lab group (LPCA).

### Correction to earlier notes

Earlier estimates (PSS ≈ 0.92, precision ≈ 0.81, base rate 0.20) assumed the 95 % accuracy was over all 730 days. It is over Apr–Sep 2019 only (182 days). Use the values above.

---

## 3. Krishnamoorthy, Sharma, Namboodiri & Goyal (2023)

*A machine learning approach to forecast wind speed, turbulence intensity, and sea breeze for offshore wind farm applications.* SSRN 4620501. **Preprint, not peer reviewed** (watermark on every page). https://ssrn.com/abstract=4620501

**Correction to earlier notes:** this study is **not** on Long Island. The arXiv paper 2309.01803 is unrelated. The site is the Gulf of Khambhat, Gujarat, India.

### Design

| Item | What the paper did | Where |
|---|---|---|
| Site / regime | NIWE offshore lidar, Gulf of Khambhat, Amreli district, Gujarat, India (20.775° N, 71.669° E): **subtropical monsoon coast**, one site | Sec. 2 |
| Data | Lidar, **one year from Nov 2017**, 10-min intervals, 80 m height. RH, T, p and wind direction also used | Sec. 2 |
| Labels | **Rule-based criteria** (citing Azorin-Molina et al. 2011): wind direction 225–270°; direction change < 120°; TI > 0.12; wind-speed difference > 2; RH > 65 %; RH difference > 5; 10–18 h | Sec. 2.2 |
| Predictors (SB) | RH, temperature, **wind direction, wind speed** | Sec. 2 |
| Task | Binary SB classification. Described as "forecasted hourly" (Sec. 4), but the SVM section describes classes as "days". **Unit unclear**; lead time not stated | Sec. 3.3, 4 |
| Split | 70 % train / 30 % test. **Random vs. chronological not stated** | Sec. 3 |
| Imbalance | SVM class weights 1:10 | Sec. 3.3 |
| Models | XGBoost, Random Forest, Gradient Boosting, SVM (RBF) | Sec. 4 |

### Reported results

- ROC AUC (Fig. 10): XGBoost **0.980**, RF 0.971, GB 0.955, SVM 0.925.
- XGBoost confusion matrix (Fig. 11): TN 309, FP 6, FN 1, TP 40 (n = 356).
- Precision 0.87, recall 0.98, F1 0.92.

### Derived scores (*derived* from Fig. 11)

| Base rate | H | F | **PSS** | κ | **Precision** | Accuracy (no-skill) |
|---|---|---|---|---|---|---|
| 0.115 (41 / 356) | 0.98 | 0.02 | **0.96** | 0.91 | **0.87** | 0.98 (0.88) |

### Critical points

1. **Circularity.** The labels are defined by thresholds on wind direction, wind speed and RH (plus TI, which derives from wind-speed variability). The classifier is then given wind direction, wind speed, RH and temperature as inputs. AUC 0.98 therefore largely measures how well the model **re-learns the labelling rule from the same variables**, not sea-breeze detection against an independent reference.
2. **Test-set size.** n = 356 does not match 30 % of one year at daily (≈ 110), hourly (≈ 2,630) or 10-min (≈ 15,800) resolution, but it is close to one year of days. The paper does not explain this, so it is **unclear whether Fig. 11 is the held-out test set**.
3. **Split design.** Not stated. With a random split of a time series, neighbouring samples can fall in both training and test sets.
4. **No baseline and no independent reference.** Nothing is compared against the labelling rule itself, against climatology, or against another instrument. No second period or site.
5. **No uncertainty** (single split, no confidence intervals). The classifier differences (AUC 0.93–0.98) cannot be judged.
6. **Possible physical point for the author to judge:** the sea-breeze sector (225–270°) coincides with the SW monsoon flow at this coast. The other criteria (RH, speed change, 10–18 h) may or may not separate the two. Not discussed in the paper.

### Within-study result (valid to report)

On the same data and split, XGBoost had the highest AUC (0.98), ahead of RF (0.97), GB (0.96) and SVM (0.93). No uncertainty is given.

### Validation level

Internal split only, at one site and in one year. The split type is unknown and the labels are rule-based from overlapping inputs. This is the lowest validation level of the three studies.

### Regime note

This is the only (sub)tropical study among those cited. Sokolov and Roy are both mid-latitude (Dunkerque).

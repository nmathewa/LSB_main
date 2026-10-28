# How other papers compare ML skill numbers that come from different methods and sites

Notes for the reply to the reviewer comment on Section 3.4.1 (non-comparable skill figures).
Focus: **how** other reviews and studies actually did the comparison. Drafting is left to the author.

---

## 0. Bottom line

No field compares raw skill numbers from different sites, labels and validation designs at face value.
The literature uses five approaches, from strongest to weakest:

| # | Approach | What it needs | Is it possible for the LSB review now? |
|---|----------|---------------|-------------------------------------|
| A | **Common testbed / intercomparison**: re-run every method on the same data, labels, period, split and metric | Shared dataset + protocol | Not for the review itself (no LSB benchmark exists). Can be proposed as a research need (fits Recommendation v) |
| B | **Within-study (paired) comparisons only**: compare methods only when they were run on the same dataset in the same paper | Nothing extra | Yes |
| C | **Normalise against a same-site reference**: skill score vs. a no-skill or simple baseline on the same data; base-rate-robust metrics; recompute from contingency tables | Contingency tables / base rate from each paper | Partly (where papers report counts) |
| D | **Classify each study by validation design** (random split → temporal hold-out → other site/region) and report numbers per tier | Read each paper's methods | Yes |
| E | **Systematic-review practice**: structured extraction table, risk-of-bias / quality flags, no ranking across heterogeneous studies; meta-regression only with many studies | Extraction table | Yes (no meta-analysis with n ≈ 4) |

For a narrative review with about four primary studies, B + C + D + E is the realistic set. A goes in as a recommendation.

---

## 1. Why the four numbers cannot be compared (the diagnosis)

What I could verify about the cited studies (full texts of Roy et al. and the SSRN paper were not reachable from this session; please check against your PDFs):

| Study | Site | Inputs | Labels ("ground truth") | Task | Reported number | Status |
|---|---|---|---|---|---|---|
| Sokolov et al. 2020, JTECH 37(4), 723–739 | Dunkirk (English Channel) | In-situ wind, T, RH, p, radiation, turbulence | Events **classified manually** from a measurement campaign | **Multi-class** (sea breeze, fog, high wind, …) | "up to 80 % correctly classified events" (kNN, QDA), i.e. **overall multi-class accuracy**, not SB-specific | Peer-reviewed |
| Roy et al. 2022, Atmosphere 13(11), 1873 | Dunkirk nearshore site (LPCA) | Ultrasonic anemometer, Jan 2018–Dec 2019 | Checked against lidar | **Binary** SB-day detection, RNN | Accuracy ≈ 95 %; 67 SB days (2018), 78 (2019). Search summaries mention **88 %** of SB cases matched lidar. **Check where the 98 % sensitivity comes from** | Peer-reviewed |
| Krishnamoorthy et al. 2023 | Long Island (NY)? | — | — | — | AUC 0.98 | **SSRN preprint**. I could not locate it. A related arXiv paper (2309.01803, "Importance of overnight parameters to predict Sea Breeze on Long Island") reports a contingency table of 40 hits / 14 misses / 20 false alarms / 38 correct negatives (≈ 70 % accuracy). **Check whether this is the same work/version and which number is current** |
| Bedoya-Valestt et al. 2022 | Global | Station data | — | Deep CNN, global SB database | — | **Conference abstract: EMS2022-522, EMS Annual Meeting, Bonn (not EGU)** |

The differences, mapped to the terms the reviewer uses:

1. **Different metric**: multi-class accuracy vs. binary accuracy vs. sensitivity vs. AUC. These are not on one scale.
2. **Different class balance (base rate)**: accuracy depends on prevalence. Worked example from Roy et al.: 145 SB days out of 730 means a base rate of ≈ 20 %. A trivial "never sea breeze" classifier then scores **≈ 80 % accuracy** (if accuracy is computed over all days). So "80 %" at one site and "95 %" at another tell you nothing until each is set against its own no-skill baseline. The literature basis is Doswell et al. 1990, Hogan et al. 2010 and Ferro & Stephenson 2011 (Section 2C).
3. **Different label construction**: manual expert labels, lidar-confirmed labels and rule-based labels define different "truths". Azorin-Molina et al. (2011) show that automated and manual SB-day selection at the same stations give very different event counts. IMILAST, ARTMIP and Bourdin et al. (2022) show the same for cyclones, ARs and TCs (Section 2A).
4. **Different validation design**: random splits of autocorrelated time series inflate skill relative to temporally or spatially blocked tests (Section 2D).
5. **Non-independence**: two of the three supervised studies are from Dunkirk, so they are not independent evidence of transferability.

---

## 2. How other papers did the comparison

### A. Common testbed / intercomparison projects (the "gold standard")

These were set up because detection results from different groups using different data were not comparable. That is the same problem as here, with "detector" in place of "ML model".

**IMILAST: Neu et al. (2013), BAMS 94(4), 529–547, doi:10.1175/BAMS-D-11-00154.1**
- *How they compared:* 15 extratropical cyclone detection/tracking schemes were all run on **the same reanalysis (ERA-Interim, 1989–2009)** with a common output format. Results were compared on the same diagnostics: track density, counts, interannual variability, trends and case studies.
- *Finding:* methods agree on the spatial patterns and variability but disagree strongly on **total numbers and weak systems**. Event counts depend on the definition.
- *Mapping to LSB:* this is the problem of SB-day counts depending on the identification criterion.

**ARTMIP: Shields et al. (2018), GMD 11, 2455–2474, doi:10.5194/gmd-11-2455-2018; Rutz et al. (2019), JGR-Atmos 124**
- *How they compared:* a tiered protocol. **Tier 1** is mandatory: every AR detection algorithm runs on the **same MERRA-2 dataset (1980–2017)** in a common format. **Tier 2** covers optional sensitivity tests (other reanalyses, climate runs). A short pilot month (Feb 2017) was used to test the framework.
- *Key idea:* uncertainty attributable to the detection definition is quantified explicitly rather than hidden.

**Bourdin et al. (2022), GMD 15, 6759–6786, doi:10.5194/gmd-15-6759-2022**
- *How they compared:* four TC trackers run on **ERA5**, all matched against **one reference (IBTrACS)**. Detection and false-alarm rates were computed the same way for all four, then the trackers were harmonised with common filters.
- *Finding:* POD ≈ 80 % for all, but false-alarm rates differ hugely (some > number of real TCs) until filtered. **Algorithm choice changes the climatology**. Shows why POD/sensitivity alone is not a comparison.

**WeatherBench: Rasp et al. (2020), JAMES, doi:10.1029/2020MS002203; WeatherBench 2: Rasp et al. (2024), JAMES 16**
- *Motivation, stated explicitly:* "the lack of a common dataset and evaluation metrics make inter-comparison between studies difficult."
- *How they compared:* a fixed dataset (regridded ERA5), a **fixed train/validation/test split by years**, a fixed metric (RMSE of Z500/T850 at 3 and 5 days), and **baselines** (climatology, persistence, linear regression, NWP) scored on the same test set.

**Dueben et al. (2022), AIES, "Challenges and benchmark datasets for machine learning in the atmospheric sciences: definition, status, and outlook"**
- A *recipe* for an atmospheric ML benchmark: a defined task, a public dataset, a fixed split, defined metrics, and baselines. Useful to cite when recommending an LSB benchmark.

**ClimateNet: Prabhat et al. (2021), GMD 14, 107–124, doi:10.5194/gmd-14-107-2021**
- Built **expert-labelled** ground truth for TCs/ARs because heuristic labels disagree. The paper cites a 3–7× spread in ETC counts across methods. This is the label-definition and ground-truth-standard point from the reviewer.

**Hydrology analogue: CAMELS + Kratzert et al. (2019), WRR 55, 11344–11354, doi:10.1029/2019WR026065**
- *How they compared:* the same 531 CAMELS basins and period for all models. **k-fold over basins**, so each basin is predicted by a model that never saw it (out-of-site test). The out-of-site LSTM (median NSE 0.69) was compared with calibrated SAC-SMA (0.64) and the National Water Model (0.58) on the same basins.
- *Mapping to LSB:* the template for a **leave-one-site-out** transfer test.

### B. Within-study (paired) comparisons only

**Khatami, Mountrakis & Stehman (2016), Remote Sensing of Environment 177**: meta-analysis of supervised land-cover classification.
- *How they compared:* they **did not compare accuracies across papers**. They used only articles that tested ≥ 2 classifiers or ≥ 2 input configurations **on the same dataset**, and analysed the **paired within-study differences** (e.g. SVM vs. maximum likelihood in the same study). Site, sensor and labels cancel out because both arms share them.

**Liu et al. (2019), Lancet Digital Health 1(6), e271–e297**: deep learning vs. clinicians in medical imaging.
- *How they compared:* from a large screened pool, pooled estimates were restricted to the **14 studies that evaluated DL and clinicians on the same test sample**. They flagged that only 25 studies had out-of-sample external validation.

*Mapping to LSB:* "kNN and QDA outperformed SVM on the Dunkirk data (Sokolov et al. 2020)" is a valid statement. "RNN (95 %) outperformed kNN (80 %)" across two papers is not.

### C. Normalise against a same-site reference (skill scores, base-rate-robust metrics)

**Wind power: Madsen et al. (2005), Wind Engineering 29(6), 475–489, "Standardizing the performance evaluation of short-term wind power prediction models"**
- *Why:* papers reported errors normalised differently (by capacity, by mean production) at different sites, so the numbers were not comparable.
- *How:* a standard protocol: fixed error definitions, normalisation by installed capacity, and **improvement over a reference model (persistence) evaluated on the same site and period**.

**Solar: Yang et al. (2020), Solar Energy 210, "Verification of deterministic solar forecasts"**
- *How:* a skill score against a standard reference (an optimal convex combination of climatology and persistence) computed **at each site**. They state that this allows comparison across models, locations and periods only "with appropriate caveats".
- Follow-up: Nguyen & Müsgens (arXiv 2208.10536; **preprint**) pooled ≈ 4,700 skill-score values from 188 papers. They pooled skill scores rather than raw RMSE for exactly this reason, and still had to stratify by forecast horizon.

**For yes/no event detection (the SB case), forecast-verification literature:**
- Doswell, Davies-Jones & Keller (1990), *Wea. Forecasting* 5, 576–585: for rare events, accuracy (percent correct) is dominated by correct negatives. Use skill scores relative to chance (Heidke, Peirce/Hanssen–Kuipers).
- Stephenson (2000), *Wea. Forecasting* 15, 221–232: the odds ratio as a skill measure that is less sensitive to the base rate.
- Hogan, Ferro, Jolliffe & Stephenson (2010), *Wea. Forecasting* 25, 710–726: even the "equitable" threat score is not equitable for rare events. The base rate matters.
- Ferro & Stephenson (2011), *Wea. Forecasting* 26, 699–713: EDI/SEDI, which are **base-rate independent** and hard to hedge.
- Saito & Rehmsmeier (2015), *PLoS ONE* 10(3), e0118432: on imbalanced data, ROC/AUC looks good while precision is poor. Report PR curves too. (Relevant to "AUC 0.98".)
- Chicco & Jurman (2020), *BMC Genomics* 21, 6: MCC is more informative than accuracy or F1 under imbalance.
- **Limit:** Leeflang et al. (2013), *CMAJ* 185(11), E537–E544. Even sensitivity and specificity shift with prevalence and case mix. Normalisation reduces non-comparability but does not remove it.

*How this is applied in practice:* for each study, extract or reconstruct the **2×2 contingency table** (hits, misses, false alarms, correct negatives) and the **base rate**. Then report POD, POFD, Peirce skill score or HSS, or at least the **no-skill accuracy** next to the reported accuracy (e.g. Roy et al.: no-skill ≈ 80 % vs. reported ≈ 95 %).

### D. Classify by validation design / transfer level

**Klemeš (1986), Hydrological Sciences Journal 31(1), 13–24**: a hierarchy of tests, each harder than the last:
1. split-sample (same site, different period),
2. proxy-basin (calibrate on one site, test on a neighbouring one),
3. differential split-sample (test on contrasting climate periods),
4. proxy-basin differential split-sample (other site *and* contrasting period).
Even passing all four is necessary, not sufficient. Andréassian et al. (2009) note that in practice almost everyone stops at level 1. For LSB, the same ladder runs: random CV → held-out year → other Dunkirk-like site → other coastline/climate.

**Justice, Covinsky & Berlin (1999), Ann. Intern. Med. 130, 515–524; Altman & Royston (2000), Stat. Med. 19, 453–473**
- Distinguish **reproducibility** (internal and temporal validation, same population) from **transportability** (geographic validation, different population). In a review, the reported number is labelled with its validation tier rather than quoted bare.

**Roberts et al. (2017), Ecography 40(8), 913–929**: random CV on spatially or temporally dependent data **underestimates error**. Use blocked CV (by time, space, group).

**Sweet, Müller, Anand & Zscheischler (2023), AIES 2(4), e230026, doi:10.1175/AIES-D-23-0026.1**
- *How they compared:* the same data and model (maize yield anomalies) under **different CV strategies** (random, temporal, spatial, feature-space clusters). Skill on **held-out years and regions** changed with the CV strategy, and so did the XAI interpretation. This is a direct earth-science demonstration that the validation design alone changes the number.

**Meyer & Pebesma (2022), Nature Communications 13, 2208, doi:10.1038/s41467-022-29838-9**: a single accuracy statistic from non-independent, clustered reference data does not describe performance in new areas ("area of applicability").

**Kapoor & Narayanan (2023), Patterns 4(9), 100804**: data leakage (including temporal leakage and non-independent train/test samples) inflated results across 294 papers in 17 fields. After the leakage was fixed, ML often no longer beat simple baselines.
- *LSB-specific check:* if 10-min or hourly samples from the same day can fall in both train and test, day-level SB skill is inflated.

### E. Systematic-review practice (how reviews present heterogeneous numbers)

**Debray et al. (2017), BMJ 356, i6460, "A guide to systematic review and meta-analysis of prediction model performance"**
- *How:* extract performance **together with study characteristics**. Expect heterogeneity. Use random-effects pooling **only to describe and explain variation** (subgroups, meta-regression on case mix, design and outcome definition), not to produce a single ranking.

**Damen et al. (2019), BMJ Open 9, e026160 (meta-epidemiological study)**: across studies, model performance varies mainly with **case mix, study design, outcome (label) definitions and measurement methods**, which is the same list as the reviewer's.

**Reitsma et al. (2005), J. Clin. Epidemiol. 58, 982–990**: when pooling sensitivity and specificity from studies with different thresholds, use a bivariate model, never pooled accuracy. (Not feasible with n ≈ 4, but it shows why bare sensitivity numbers are not pooled.)

**Extraction and quality tools:** CHARMS (Moons et al. 2014, *PLoS Med* 11(10), e1001744) is the data-extraction checklist: source, outcome definition, sample size and number of events, validation type. PROBAST (Wolff et al. 2019, *Ann. Intern. Med.* 170, 51–58) is the risk-of-bias rating covering participants, predictors, outcome and analysis.

**Example reviews that did this:**
- **Reichenbach et al. (2018), Earth-Science Reviews 180, 60–91, doi:10.1016/j.earscirev.2018.03.001** (landslide susceptibility, 565 papers). They found heterogeneous data, methods and **evaluation criteria**. Instead of ranking reported AUCs, they built a **"Susceptibility Quality Level" index** to grade each study's evaluation rigour. This is the closest Earth-science analogue to the requested maturity and quality flags.
- **Roberts et al. (2021), Nature Machine Intelligence 3, 199–217, doi:10.1038/s42256-021-00307-0** (2,212 COVID-imaging ML papers screened, 62 assessed). Each study was assessed for bias (dataset construction, label source, validation). They concluded that **none** were clinically usable despite high reported AUCs. This shows that a high headline AUC was not accepted at face value.
- **Liu et al. (2019)**: see B.

**Reporting standards that define what must be reported (and therefore what a review extracts):**
- REFORMS: Kapoor et al. (2024), *Science Advances* 10, eadk3452. A 32-item checklist for ML-based science, covering data provenance, label definition, train/test separation, leakage and baselines.
- TRIPOD+AI: Collins et al. (2024), *BMJ* 385, e078378. A 27-item checklist covering outcome definition, number of events/prevalence, and validation type.

---

## 3. What this implies for Section 3.4.1 (actions, not prose)

1. **Replace the run-on quoting of numbers with a study table.** Columns: site/region; sensors/inputs; period; label source and definition (manual / lidar / rule-based, which criteria); task (binary vs. multi-class; day vs. time step); SB base rate; model; validation design (random split / CV / temporal hold-out / other site); metric and value; no-skill reference (where computable); publication status (peer-reviewed / preprint / abstract).
2. **Add one explicit non-comparability caveat** where the numbers appear, naming the four reasons: metric, base rate, label definition, validation design. Do not rank across studies.
3. **Keep only within-study comparisons** as statements about which method is better (approach B).
4. **Put each number next to its no-skill baseline** where possible (approach C). Example: Roy et al., base rate ≈ 20 %, so "always no-SB" ≈ 80 % accuracy.
5. **Short evaluation-protocol paragraph** (approach D): the Klemeš / Justice ladder (random → temporal hold-out → leave-one-site-out → other coastline/regime); blocked CV by day, season or year; report the contingency table, base rate and a simple rule-based baseline (e.g. the Azorin-Molina et al. 2011 or Borne et al. 1998 criteria) on the same data.
6. **Link to Recommendation v:** a multi-site labelled LSB benchmark with a common protocol, following ARTMIP, IMILAST, WeatherBench and the Dueben et al. (2022) recipe.
7. **Maturity flags:** mark preprint / abstract in the table (Reichenbach-style quality column). Note that the two Dunkirk studies are not independent.

---

## 4. Things to double-check in the manuscript's citations

- **Roy et al. (2022), 98 % sensitivity:** search summaries of the paper mention ≈ 95 % accuracy and **88 %** of SB cases matching lidar. Confirm where "98 %" comes from (which table, which reference truth).
- **Sokolov et al. (2020), 80 %:** this is overall multi-class accuracy ("correctly classified meteorological events": SB, fog, high wind, …), not SB detection skill. Say so.
- **Krishnamoorthy et al. (2023), AUC 0.98:** confirm the SSRN version and whether a later version (arXiv 2309.01803?) reports different figures. A ≈ 70 % accuracy contingency table appears in that arXiv paper; check whether it is the same work.
- **Bedoya-Valestt et al. (2022):** the abstract is **EMS2022-522 (EMS Annual Meeting, Bonn)**, not EGU. Check the reference entry.

---

## 5. References with links

DOI links checked in this session unless marked otherwise.

**Cited LSB ML studies**
- Sokolov, A., et al. (2020). Automated classification of regional meteorological events in a coastal area using in situ measurements. *J. Atmos. Oceanic Technol.*, 37(4), 723–739. https://doi.org/10.1175/JTECH-D-19-0120.1
- Roy, S., Sentchev, A., Fourmentin, M., & Augustin, P. (2022). *Atmosphere*, 13(11), 1873. https://www.mdpi.com/2073-4433/13/11/1873
- Bedoya-Valestt, S., et al. (2022). A deep learning approach for identifying coastal sea breezes globally. EMS Annual Meeting 2022, EMS2022-522 (abstract). https://meetingorganizer.copernicus.org/EMS2022/EMS2022-522.html
- Possibly related to Krishnamoorthy et al. (2023): "Importance of overnight parameters to predict Sea Breeze on Long Island", arXiv:2309.01803 (authorship not confirmed). https://arxiv.org/abs/2309.01803
- Azorin-Molina, C., Tijm, S., & Chen, D. (2011). Development of selection algorithms and databases for sea breeze studies. *Theor. Appl. Climatol.*, 106, 531–546. https://doi.org/10.1007/s00704-011-0454-4

**A. Intercomparison / benchmarks**
- Neu, U., et al. (2013). IMILAST. *BAMS*, 94(4), 529–547. https://doi.org/10.1175/BAMS-D-11-00154.1
- Shields, C. A., et al. (2018). ARTMIP: project goals and experimental design. *GMD*, 11, 2455–2474. https://doi.org/10.5194/gmd-11-2455-2018
- Rutz, J. J., et al. (2019). ARTMIP: Quantifying uncertainties in atmospheric river climatology. *JGR: Atmospheres*, 124, 13777–13802. https://doi.org/10.1029/2019JD030936
- Bourdin, S., et al. (2022). Intercomparison of four algorithms for detecting tropical cyclones using ERA5. *GMD*, 15, 6759–6786. https://doi.org/10.5194/gmd-15-6759-2022
- Rasp, S., et al. (2020). WeatherBench. *JAMES*, 12, e2020MS002203. https://doi.org/10.1029/2020MS002203
- Rasp, S., et al. (2024). WeatherBench 2. *JAMES*, 16. (link not checked)
- Dueben, P. D., et al. (2022). Challenges and benchmark datasets for machine learning in the atmospheric sciences. *AIES*. https://par.nsf.gov/biblio/10422671
- Prabhat, et al. (2021). ClimateNet. *GMD*, 14, 107–124. https://doi.org/10.5194/gmd-14-107-2021
- Kratzert, F., et al. (2019). Toward improved predictions in ungauged basins. *WRR*, 55, 11344–11354. https://doi.org/10.1029/2019WR026065

**B. Within-study comparisons**
- Khatami, R., Mountrakis, G., & Stehman, S. V. (2016). *Remote Sensing of Environment*, 177. https://researchconnect.suny.edu/en/publications/a-meta-analysis-of-remote-sensing-research-on-supervised-pixel-ba/
- Liu, X., et al. (2019). *Lancet Digital Health*, 1(6), e271–e297. https://doaj.org/article/c73287f0c1f24d9abbb2cb81ce6add3e

**C. Skill vs. reference / metrics**
- Madsen, H., et al. (2005). Standardizing the performance evaluation of short-term wind power prediction models. *Wind Engineering*, 29(6), 475–489. https://doi.org/10.1260/030952405776234599
- Yang, D., et al. (2020). Verification of deterministic solar forecasts. *Solar Energy*, 210, 20–37. https://doi.org/10.1016/j.solener.2020.04.019
- Nguyen, T. N., & Müsgens, F. (2022). A meta-analysis of solar forecasting based on skill score. arXiv (preprint). https://arxiv.org/abs/2208.10536
- Doswell, C. A., Davies-Jones, R., & Keller, D. L. (1990). *Weather and Forecasting*, 5, 576–585. https://scilit.com/publications/acec8c5c6233ab1581088ac991cfac18
- Stephenson, D. B. (2000). Use of the "odds ratio" for diagnosing forecast skill. *Weather and Forecasting*, 15, 221–232. PDF: https://stormrisk.github.io/2000/wf2000.pdf
- Hogan, R. J., et al. (2010). Equitability revisited. *Weather and Forecasting*, 25, 710–726. PDF: https://core.ac.uk/download/9334.pdf
- Ferro, C. A. T., & Stephenson, D. B. (2011). Extremal dependence indices. *Weather and Forecasting*, 26, 699–713. https://ore.exeter.ac.uk/repository/handle/10871/8521
- Saito, T., & Rehmsmeier, M. (2015). *PLoS ONE*, 10(3), e0118432. https://pmc.ncbi.nlm.nih.gov/articles/PMC4349800
- Chicco, D., & Jurman, G. (2020). *BMC Genomics*, 21, 6. https://doi.org/10.1186/s12864-019-6413-7
- Leeflang, M. M. G., et al. (2013). Variation of a test's sensitivity and specificity with disease prevalence. *CMAJ*, 185(11), E537–E544. https://doi.org/10.1503/cmaj.121286

**D. Validation design / transfer**
- Klemeš, V. (1986). Operational testing of hydrological simulation models. *Hydrol. Sci. J.*, 31(1), 13–24. https://agris.fao.org/search/en/records/65df636b63b8185d9cacb2f4
- Andréassian, V., et al. (2009). Crash tests for a standardized evaluation of hydrological models. *HESS*, 13, 1757–1764. PDF: https://opac.geologie.ac.at/ais312/dokumente/copernicus.org/hess-13-1757-2009.pdf
- Justice, A. C., Covinsky, K. E., & Berlin, J. A. (1999). *Ann. Intern. Med.*, 130(6), 515–524. https://doi.org/10.7326/0003-4819-130-6-199903160-00016
- Altman, D. G., & Royston, P. (2000). What do we mean by validating a prognostic model? *Stat. Med.*, 19, 453–473. https://ora.ox.ac.uk/objects/uuid:add14c28-8561-45a9-9cc7-4b00258b0929
- Roberts, D. R., et al. (2017). Cross-validation strategies for data with temporal, spatial, hierarchical, or phylogenetic structure. *Ecography*, 40(8), 913–929. https://doi.org/10.1111/ecog.02881
- Sweet, L., et al. (2023). Cross-validation strategy impacts the performance and interpretation of ML models. *AIES*, 2(4), e230026. https://doi.org/10.1175/AIES-D-23-0026.1
- Meyer, H., & Pebesma, E. (2022). *Nature Communications*, 13, 2208. https://doi.org/10.1038/s41467-022-29838-9
- Kapoor, S., & Narayanan, A. (2023). Leakage and the reproducibility crisis in ML-based science. *Patterns*. https://pmc.ncbi.nlm.nih.gov/articles/PMC10499856

**E. Review practice / reporting standards**
- Debray, T. P. A., et al. (2017). A guide to systematic review and meta-analysis of prediction model performance. *BMJ*, 356, i6460. https://doi.org/10.1136/bmj.i6460
- Damen, J. A. A. G., et al. (2019). *BMJ Open*, 9, e026160. https://www.rti.org/publication/empirical-evidence-impact-study-characteristics-performance-prediction-models-meta-epidemiological-s
- Reitsma, J. B., et al. (2005). *J. Clin. Epidemiol.*, 58(10), 982–990. https://doi.org/10.1016/j.jclinepi.2005.02.022
- Moons, K. G. M., et al. (2014). CHARMS checklist. *PLoS Medicine*, 11(10), e1001744. https://doi.org/10.1371/journal.pmed.1001744
- Wolff, R. F., et al. (2019). PROBAST. *Ann. Intern. Med.*, 170(1), 51–58. https://doi.org/10.7326/M18-1376
- Reichenbach, P., et al. (2018). A review of statistically-based landslide susceptibility models. *Earth-Science Reviews*, 180, 60–91. https://doi.org/10.1016/j.earscirev.2018.03.001
- Roberts, M., et al. (2021). *Nature Machine Intelligence*, 3, 199–217. https://doi.org/10.1038/s42256-021-00307-0
- Kapoor, S., et al. (2024). REFORMS. *Science Advances*. https://reforms.cs.princeton.edu/
- Collins, G. S., et al. (2024). TRIPOD+AI. *BMJ*, 385, e078378. https://www.ncbi.nlm.nih.gov/pmc/articles/PMC11025451/

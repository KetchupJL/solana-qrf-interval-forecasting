# Interval Forecasting of Cryptocurrency Returns using Quantile Regression Forests

[![ResearchGate](https://img.shields.io/badge/ResearchGate-Read%20Paper-brightgreen?logo=researchgate)](https://www.researchgate.net/publication/395025657_Interval_Forecasting_of_Cryptocurrency_Returns_using_Quantile_Regression_Forests_An_Application_to_the_Solana_Ecosystem)
[![DOI](https://img.shields.io/badge/DOI-10.13140%2FRG.2.2.29811.59687-blue)](https://doi.org/10.13140/RG.2.2.29811.59687)
[![License: CC BY-NC-ND 4.0](https://img.shields.io/badge/License-CC%20BY--NC--ND%204.0-lightgrey.svg)](https://creativecommons.org/licenses/by-nc-nd/4.0/)

---

### Project Overview

This repository hosts the full research pipeline for my MSc dissertation:  
**“Interval Forecasting of Cryptocurrency Returns using Quantile Regression Forests: An Application to the Solana Ecosystem.”**

The study addresses a central question in quantitative finance:

> Can an adapted Quantile Regression Forest (QRF) framework provide sharper, better-calibrated return intervals for mid-cap Solana tokens compared to Linear Quantile Regression (LQR) and LightGBM baselines?

Unlike traditional point forecasts, the focus here is on **distributional accuracy** and **calibrated risk intervals**, which are essential for **quant trading and risk management** in highly volatile, heavy-tailed markets like crypto.

---

### Key Contributions

- **Methodological innovation**:  
  Adapted QRF pipeline including:
  - Time-decay sample weights (recency emphasis),  
  - Isotonic rearrangement for non-crossing quantiles,  
  - Regime-aware residual calibration with conformal top-up.  

- **Rigorous evaluation**:  
  Blocked rolling CV (120/24/6), per-quantile pinball loss, calibration reliability, coverage–width efficiency, and Diebold–Mariano tests.  

- **Empirical insight**:  
  - QRF achieves the lowest pinball loss in risk-critical tails (τ = 0.05, 0.10).  
  - After calibration, QRF reaches ~0.88 coverage at 90% intervals with narrower bands than LightGBM.  
  - Intervals adapt to volatility regimes — widening in turbulent markets, contracting in stable ones.  

- **Applied trading relevance**:  
  Risk-aware sizing strategies based on QRF intervals improve Sharpe ratios and reduce drawdowns versus fixed-size benchmarks.  
  This demonstrates the direct applicability of academic interval-forecasting methods to **algorithmic trading strategy design**.

---

### Results at a Glance

| Metric                          | QRF (final) | LightGBM (tuned) | LQR |
|---------------------------------|-------------|------------------|-----|
| Pinball loss @ τ=0.10           | **0.022**   | 0.055            | 0.041 |
| Coverage (90% band)             | **0.878**   | 0.979            | 0.622 |
| Mean width (90% band)           | 0.63        | 0.92             | 0.58 |
| Sharpe (interval sizing rule)   | **0.92**    | 0.74             | 0.65 |

---

### Repository Structure

```
├── data/                 # Cleaned datasets (OHLCV, on-chain, features)
├── notebooks/            # Jupyter notebooks (EDA, modelling, calibration, results)
├── paper/                # Dissertation text and figures
│   └── figures/final/    # Key plots (fan charts, calibration curves, efficiency scatter)
├── src/                  # Python modules for data, feature engineering, modelling
├── results/              # Tables, backtest outputs, significance tests
└── README.md
```

---

### Selected Figures

Representative results are included in `paper/figures/final/`. For example:

- **Quantile Spaghetti Plot (AVA token)**  
  ![Quantile Spaghetti Plot](paper/figures/final/fig_quantile_spaghetti_ava.pdf)

- **Calibration curve (empirical vs nominal coverage)**
  ![Calibration curve Plot](paper/figures/final/fig_calibration_curve.pdf)  

- **Sharpness–coverage efficiency scatter**
  ![Sharpness-coverage scatter](paper/figures/final/fig_efficiency_scatter.pdf)


- **Representative fan chart (per-token intervals)**
  ![Medium + 80% pred intervals for MEW](paper/figures/final/fig-mew-80preds.pdf.pdf)

---

### Tools & Technologies

| Category | Tools |
|----------|-------|
| Modelling | [Quantile-Forest (Cython)](https://github.com/zillow/quantile-forest), [LightGBM](https://github.com/microsoft/LightGBM), [statsmodels QuantReg](https://www.statsmodels.org/) |
| Calibration | [MAPIE](https://github.com/scikit-learn-contrib/MAPIE) (conformal intervals), custom isotonic & regime-aware adjustments |
| Data | Pandas, NumPy, Google BigQuery (Solana chain data), SolanaTracker API, CoinGecko API |
| Evaluation | Pinball loss, Diebold–Mariano tests, Sharpness–Coverage efficiency |
| Visualisation | Matplotlib, Seaborn |

---

### Research Questions

1. Do QRFs deliver **lower pinball loss** across quantiles than LQR and LightGBM?  
2. Can calibrated QRF intervals achieve **nominal coverage with narrower widths** across regimes and tokens?  
3. Do statistically superior intervals translate into **economic value in trading backtests**?  

---

### Citation

If you use this work, please cite:

> James Lewis (2025). *Interval Forecasting of Cryptocurrency Returns using Quantile Regression Forests: An Application to the Solana Ecosystem.* MSc Dissertation, University of Exeter. DOI: [10.13140/RG.2.2.29811.59687](https://doi.org/10.13140/RG.2.2.29811.59687)

```bibtex
@article{Lewis2025QRF,
  author  = {James Lewis},
  title   = {Interval Forecasting of Cryptocurrency Returns using Quantile Regression Forests: An Application to the Solana Ecosystem},
  year    = {2025},
  doi     = {10.13140/RG.2.2.29811.59687},
  journal = {Preprint on ResearchGate}
}
```

---

### License

This repository is licensed under the terms of the [CC BY-NC-ND 4.0 License](https://creativecommons.org/licenses/by-nc-nd/4.0/).

---

*This project reflects my strong research interest in **machine learning for financial forecasting**, with a focus on **quantitative risk modelling** and **algorithmic trading applications**.*


# Risk-Adjusted Interval Forecasting of Mid‑Cap Solana Token Returns Using QRF

[![Build Status](https://img.shields.io/github/actions/workflow/status/KetchupJL/solana-qrf/ci.yml)](https://github.com/KetchupJL/solana-qrf/actions)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](./LICENSE)

A research‑oriented repository providing data ingestion pipelines, feature engineering, modeling scripts, and evaluation tools for interval forecasting of 72‑hour returns on mid‑cap Solana tokens (≥ $50M market cap) using Quantile Regression Forests, with comparisons to Linear Quantile Regression and LightGBM baselines.

---

## 📂 Repository Structure

```
├── README.md                  # Project overview and setup
├── LICENSE                    # MIT License
├── .gitignore                 # Files and folders to ignore in git
├── requirements.txt           # Python dependencies
├── data/
│   ├── raw/                   # Unmodified downloaded data (OHLCV, order-book, on-chain)
│   └── processed/             # Cleaned, binned, and feature‑ready datasets
├── notebooks/                 # Jupyter notebooks for EDA and prototyping
│   ├── 01_data_ingestion.ipynb
│   ├── 02_feature_engineering.ipynb
│   └── 03_preliminary_analysis.ipynb
├── src/                       # Python modules
│   ├── data/                  # Data download & preprocessing functions
│   ├── features/              # Feature computation routines
│   ├── models/                # Model definitions & training scripts
│   └── utils/                 # Helper functions (logging, config parsing)
├── results/
│   ├── figures/               # Generated plots (calibration, coverage curves)
│   └── tables/                # Evaluation metrics outputs (pinball loss, coverage)
├── docs/                      # Supplementary documentation & design notes
├── tests/                     # Unit tests for critical functions
└── ci.yml                     # GitHub Actions CI pipeline definition
```

---

## 🚀 Getting Started

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/solana-qrf.git
   cd solana-qrf
   ```

2. **Create a virtual environment & install dependencies**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   - Copy `.env.example` to `.env` and fill in API keys for DEX, The Graph, Twitter/Telegram, etc.

4. **Run data ingestion pipeline**
   ```bash
   python -m src.data.ingest --start-date YYYY-MM-DD --end-date YYYY-MM-DD
   ```

---

## 🛠️ Usage Examples

- **Feature engineering**: compute features on processed data:
  ```bash
  python -m src.features.compute --input data/processed --output data/processed/features.csv
  ```

- **Train models**:
  ```bash
  python -m src.models.train --model qrf --config configs/qrf.yaml
  ```

- **Evaluate & plot results**:
  ```bash
  python -m src.models.evaluate --predictions results/predictions.csv --ground-truth data/processed/72h_returns.csv
  ```

---

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork this repository.
2. Create a new branch: `git checkout -b feature/your-feature-name`.
3. Implement your change and add tests if necessary.
4. Commit your changes: `git commit -m "Add your feature"`.
5. Push to your fork: `git push origin feature/your-feature-name`.
6. Open a Pull Request describing your changes.

Please adhere to the [Code of Conduct](CODE_OF_CONDUCT.md) and the [Style Guide](docs/style_guide.md).

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](./LICENSE) for details.

---

## ✉️ Contact

For questions or collaboration inquiries, please open an issue or contact:

- **James R Lewis** – james066lewis@gmail.com
- GitHub: [@KetchupJL](https://github.com/KetchupJL)

Happy forecasting!

"""
Recreate the Linear Quantile Regression (LQR) baseline with an
extended set of quantiles matching those used in the tuned QRF model.

This script mirrors the logic of the original LQR implementation
provided by the user but expands the quantile grid to include
{0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95}.  It also enforces
non‐crossing predictions by sorting the predicted quantiles for each
test instance.  The rolling cross‐validation scheme (train–test
splits) remains unchanged: for each token, we train on 120 rows
and test on the next 30 rows, stepping forward by 30 rows per fold.

To run this script you will need to have a `df` DataFrame defined in
your environment containing the engineered features and the target
column `return_72h`.  The variable `feat_cols` should list all
feature column names.  Categorical columns are assumed to include
`token`, `momentum_bucket` and `day_of_week`.

Outputs:
  - `lqr_pred_paths_full.csv`: per‐row predictions for each quantile
    and true value.
  - `lqr_fold_metrics_full.csv`: per‐fold pinball losses and 80 % interval
    coverage/width.

This file is provided as a standalone example; adjust paths and
column names as needed for your environment.
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, OneHotEncoder
from sklearn.compose import ColumnTransformer
from statsmodels.regression.quantile_regression import QuantReg as QR

def run_lqr_with_extended_quantiles(df: pd.DataFrame, feat_cols: list) -> None:
    """Run linear quantile regression with an extended quantile grid.

    Parameters
    ----------
    df : DataFrame
        The input dataframe containing features, a `return_72h` column,
        a `timestamp` column, and a `token` column.
    feat_cols : list
        List of all feature column names to be used for modelling.

    The function saves predictions and fold metrics to CSV files in
    the current working directory.  Adjust paths as needed.
    """
    # Quantiles to estimate (match QRF setup)
    quantiles = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]

    # Separate numerical and categorical columns
    num_cols = [c for c in feat_cols if df[c].dtype != "object"]
    cat_cols = ["token", "momentum_bucket", "day_of_week"]

    # Preprocessor: scale numeric features and one‐hot encode categoricals
    pre = ColumnTransformer([
            ("num", StandardScaler(), num_cols),
            ("cat", OneHotEncoder(drop="first", handle_unknown="ignore"), cat_cols)
        ],
        remainder="drop"
    )

    # Rolling parameters
    horizon = 30       # rows in each test fold
    train_window = 120 # rows in each training window

    fold_metrics = []  # one row per fold × token × τ
    pred_records = []  # one row per observation in every test fold

    for tkn, g in df.groupby("token"):
        g = g.reset_index(drop=True)
        fold_num = 0
        for start in range(0, len(g) - (train_window + horizon) + 1, horizon):
            fold_num += 1
            # Define train/test slices
            train = g.iloc[start : start + train_window]
            test  = g.iloc[start + train_window : start + train_window + horizon]

            # Fit preprocessing on the training slice only
            X_train = pre.fit_transform(train)
            X_test  = pre.transform(test)
            y_train = train["return_72h"].values
            y_test  = test["return_72h"].values

            # Fit quantile regression models for each quantile
            preds = {}
            for q in quantiles:
                model = QR(y_train, X_train).fit(q=q, max_iter=5000)
                preds[q] = model.predict(X_test)

            # Enforce monotonicity by sorting predicted quantiles rowwise
            # Stack predictions into an array of shape (n_samples, n_quantiles)
            pred_array = np.vstack([preds[q] for q in quantiles]).T
            pred_array_sorted = np.sort(pred_array, axis=1)
            for idx, q in enumerate(quantiles):
                preds[q] = pred_array_sorted[:, idx]

            # Compute 80% interval coverage and width using q10 and q90
            lower = preds[0.10]
            upper = preds[0.90]
            inside80 = ((y_test >= lower) & (y_test <= upper))
            fold_metrics.append({
                "token": tkn,
                "fold": fold_num,
                "tau": "80PI",
                "coverage": inside80.mean(),
                "width": (upper - lower).mean(),
            })

            # Compute pinball loss for each quantile
            for q in quantiles:
                err = y_test - preds[q]
                pinball = np.maximum(q * err, (q - 1) * err).mean()
                fold_metrics.append({
                    "token": tkn,
                    "fold": fold_num,
                    "tau": q,
                    "pinball": pinball,
                })

            # Record per‐row predictions
            for i in range(len(test)):
                rec = {
                    "token": tkn,
                    "timestamp": test.iloc[i]["timestamp"],
                    "fold": fold_num,
                    "y_true": y_test[i],
                }
                # add predictions for each quantile
                for q in quantiles:
                    key = f"q{int(q * 100):02d}_pred"
                    rec[key] = preds[q][i]
                pred_records.append(rec)

    # Save results
    pred_df = pd.DataFrame(pred_records)
    metrics_df = pd.DataFrame(fold_metrics)
    pred_df.to_csv("lqr_pred_paths_full.csv", index=False)
    metrics_df.to_csv("lqr_fold_metrics_full.csv", index=False)
    print("Finished! Predictions → lqr_pred_paths_full.csv; metrics → lqr_fold_metrics_full.csv")


if __name__ == "__main__":
    # NOTE: This block is for demonstration and will not run as
    # `df` and `feat_cols` are not defined in this module.  Import
    # this function into your analysis notebook or script where `df`
    # and `feat_cols` are defined, then call:
    # run_lqr_with_extended_quantiles(df, feat_cols)
    pass
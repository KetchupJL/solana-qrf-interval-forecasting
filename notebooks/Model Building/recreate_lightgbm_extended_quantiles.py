"""
Recreate the LightGBM quantile regression model with an extended
quantile grid matching that used for the tuned QRF model.

This script modifies the original LightGBM v4 baseline to predict
additional quantiles (0.05 and 0.95) alongside the existing
quantiles (0.10, 0.25, 0.50, 0.75, 0.90).  The conformal calibration
is retained for the 80 % interval (0.10–0.90) via an adaptive
λ adjustment.  The new extreme quantiles (0.05, 0.95) are produced
directly by their respective models without additional calibration.

To use this script, ensure you have run Optuna to obtain best
hyperparameters for each quantile and saved them in
`best_lgb_cqr_params.json`.  The function `run_lightgbm_extended`
loads the feature matrix, applies rolling cross‐validation, trains
LightGBM quantile regressors for the required quantiles, calibrates
the 10th and 90th percentiles, interpolates intermediate quantiles,
and saves per‐row predictions and per‐fold pinball metrics to
CSV files.

Note: This script is provided as a template and does not execute on
its own because it depends on local files and environment setup.
Import and adapt the function into your analysis notebook or pipeline
where `features_v1_tail.csv` and parameter JSON are available.
"""
import gc
import json
import os
import itertools
import numpy as np
import pandas as pd
import lightgbm as lgb
from joblib import Parallel, delayed
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from tqdm import tqdm


def run_lightgbm_extended(
    data_file: str,
    param_file: str,
    target: str = "return_72h",
    cover: float = 0.80,
    train_len: int = 120,
    cal_len: int = 24,
    test_len: int = 6,
    n_jobs: int = None
) -> None:
    """Run LightGBM quantile regression with an extended quantile grid.

    Parameters
    ----------
    data_file : str
        Path to the cleaned feature matrix (CSV) with columns
        including `timestamp`, `token`, and the target variable.
    param_file : str
        JSON file containing Optuna‐tuned hyperparameters per
        quantile.  Keys may be numbers or strings; they will be
        matched approximately.
    target : str, default 'return_72h'
        Name of the target column to be forecasted.
    cover : float, default 0.80
        Desired coverage for the central prediction interval (used
        when calibrating the 0.10 and 0.90 quantiles).
    train_len : int, default 120
        Number of rows in each rolling training window.
    cal_len : int, default 24
        Number of rows in the calibration window.
    test_len : int, default 6
        Number of rows in the test window.
    n_jobs : int, optional
        Number of parallel jobs for model fitting.  Defaults to
        `os.cpu_count() - 2` if not specified.

    The function writes predictions to `lgb_extended_preds.csv` and
    per‐fold metrics to `lgb_extended_pinball.csv`.
    """

    # -------------- 1. load data and parameters -----------------
    df = (
        pd.read_csv(data_file, parse_dates=["timestamp"])
          .sort_values(["token", "timestamp"])
          .reset_index(drop=True)
    )

    # Separate feature columns
    cat_cols = ["token", "momentum_bucket", "day_of_week"]
    num_cols = [c for c in df.columns if c not in cat_cols + ["timestamp", target]]

    # Cast categoricals to category for LightGBM native handling
    for c in cat_cols:
        df[c] = df[c].astype("category")

    # Load best parameters per quantile
    with open(param_file) as f:
        best_params = json.load(f)

    # Quantiles to fit (matching QRF grid)
    quantiles = [0.05, 0.10, 0.25, 0.50, 0.75, 0.90, 0.95]

    # Compute tail α for 80 % coverage
    alpha_tail = (1.0 - cover) / 2.0

    # Column transformer: one‐hot encode categoricals, passthrough numerics
    pre = ColumnTransformer([
        ("cat", OneHotEncoder(drop="first", handle_unknown="ignore"), cat_cols),
        ("num", "passthrough", num_cols),
    ])

    # Rolling splits generator
    def rolling_splits(idx):
        step = test_len
        for start in range(0, len(idx) - (train_len + cal_len + test_len) + 1, step):
            tr = idx[start : start + train_len]
            cal = idx[start + train_len : start + train_len + cal_len]
            te = idx[start + train_len + cal_len : start + train_len + cal_len + test_len]
            if len(te) == test_len:
                yield tr, cal, te

    def params_for_tau(tau: float) -> dict:
        """Retrieve tuned parameters for a given quantile tau.

        Attempts to match the key exactly (as float or string) or
        finds the nearest key if no exact match exists.  The returned
        dictionary is a copy with `alpha` set to the true tau.
        """
        # Try to match exact keys (float, str, formatted float)
        for k in (tau, str(tau), f"{tau:.2f}", f"{tau:.3f}"):
            if k in best_params:
                p = best_params[k].copy()
                break
        else:
            # Fall back to nearest key by numeric value
            nearest = min(best_params.keys(), key=lambda k: abs(float(k) - tau))
            p = best_params[nearest].copy()
        p["alpha"] = tau
        return p

    # Conformal adjustment for individual quantiles (not used for 0.10/0.90 calibration)
    def cqr_adjust(pred_te, resid_cal, tau):
        """Adjust quantile predictions using conformal residuals.

        For a lower quantile (tau < 0.5), subtract the (1−alpha_tail)
        quantile of positive residuals from the test predictions.  For an
        upper quantile (tau > 0.5), add the (1−alpha_tail) quantile of
        positive residuals (of the negative residuals) to the test
        predictions.  The median (tau=0.5) is returned unchanged.

        This function mirrors the behaviour of the original cqr_adjust
        helper.
        """
        if tau < 0.5:
            r_plus = np.maximum(resid_cal, 0.0)
            q_adj = np.quantile(r_plus, 1 - alpha_tail)
            return pred_te - q_adj
        elif tau > 0.5:
            r_plus = np.maximum(-resid_cal, 0.0)
            q_adj = np.quantile(r_plus, 1 - alpha_tail)
            return pred_te + q_adj
        else:
            return pred_te

    # Compute adaptive λ for 80 % coverage on calibration slice
    def find_lambda(lower, upper, y_cal, cover=0.80):
        λ = 0.0
        # Heuristic step size based on the 75th percentile of interval widths
        step = np.percentile(upper - lower, 75) * 0.02
        if step <= 0:
            step = 1e-4
        while True:
            inside = ((y_cal >= (lower - λ)) & (y_cal <= (upper + λ))).mean()
            if inside >= cover or λ > 10.0:
                return λ
            λ += step

    # Fit and predict for one fold
    def fit_one_fold(g, tr_idx, cal_idx, te_idx):
        X_tr = pre.fit_transform(g.loc[tr_idx, cat_cols + num_cols]).astype("float32")
        y_tr = g.loc[tr_idx, target].values
        X_cal = pre.transform(g.loc[cal_idx, cat_cols + num_cols]).astype("float32")
        y_cal = g.loc[cal_idx, target].values
        X_te = pre.transform(g.loc[te_idx, cat_cols + num_cols]).astype("float32")
        y_te = g.loc[te_idx, target].values

        token_id = g["token"].iloc[0]
        fold_pred, fold_res = [], []

        # Fit base models for each quantile separately
        base_models, base_preds_cal, base_preds_te = {}, {}, {}
        for tau in [0.05, 0.10, 0.50, 0.90, 0.95]:
            p = params_for_tau(tau)
            # Update with common training hyperparameters
            p.update(num_iterations=4000, early_stopping_round=200, verbose=-1)
            mdl = lgb.LGBMRegressor(**p)
            mdl.fit(X_tr, y_tr, eval_set=[(X_cal, y_cal)], eval_metric="quantile")
            base_models[tau] = mdl
            base_preds_cal[tau] = mdl.predict(X_cal)
            base_preds_te[tau] = mdl.predict(X_te)

        # Conformal adjustment for extreme quantiles (0.05, 0.95) on calibration residuals
        adjusted_te = {}
        for tau in [0.05, 0.10, 0.50, 0.90, 0.95]:
            resid_cal = y_cal - base_preds_cal[tau]
            adjusted_te[tau] = cqr_adjust(base_preds_te[tau], resid_cal, tau)

        # Apply adaptive λ to 0.10 and 0.90 quantiles to ensure central coverage
        lower_cal = adjusted_te[0.10]
        upper_cal = adjusted_te[0.90]
        λ_star = find_lambda(lower_cal, upper_cal, y_cal, cover=cover)

        # Minimum width floor to prevent degenerate intervals
        sigma_cal = np.std(y_cal)
        min_w = 0.15 * sigma_cal
        λ_final = max(λ_star, min_w)

        # Calibrated lower and upper bounds
        lower_te = adjusted_te[0.10] - λ_final
        upper_te = adjusted_te[0.90] + λ_final
        median_te = adjusted_te[0.50]

        # ------------- aggregate predictions for all quantiles --------------
        mapping = {
            0.05: adjusted_te[0.05],
            0.10: lower_te,
            0.25: 0.25 * lower_te + 0.75 * median_te,
            0.50: median_te,
            0.75: 0.75 * median_te + 0.25 * upper_te,
            0.90: upper_te,
            0.95: adjusted_te[0.95],
        }

        for tau, preds in mapping.items():
            # Per-row predictions
            for i, yt, yp in zip(te_idx, y_te, preds):
                fold_pred.append({
                    "timestamp": g.loc[i, "timestamp"],
                    "token": token_id,
                    "tau": tau,
                    "y_true": yt,
                    "y_pred": yp,
                })
            # Pinball loss
            err = y_te - preds
            pin = np.maximum(tau * err, (tau - 1) * err).mean()
            fold_res.append({"token": token_id, "tau": tau, "pinball": pin})

        # Clean up
        del base_models
        gc.collect()
        return fold_pred, fold_res

    # Run rolling CV per token in parallel
    if n_jobs is None:
        n_jobs = max(os.cpu_count() - 2, 1)

    def run_token(group: pd.DataFrame):
        """Apply rolling splits and model fitting to a single token group.

        Returns a tuple of (predictions_list, metrics_list) where each
        element in the lists corresponds to a single fold.
        """
        group = group.reset_index(drop=True)
        token_preds, token_metrics = [], []
        for tr, cal, te in rolling_splits(group.index):
            fold_pred, fold_res = fit_one_fold(group, tr, cal, te)
            token_preds.append(fold_pred)
            token_metrics.append(fold_res)
        return token_preds, token_metrics

    # Execute in parallel over tokens
    results = Parallel(n_jobs=n_jobs, verbose=5)(
        delayed(run_token)(grp) for _, grp in tqdm(df.groupby("token"), desc="tokens")
    )

    # Flatten results
    preds_list: list = []
    metrics_list: list = []
    for token_result in results:
        token_preds, token_metrics = token_result
        for fold_pred in token_preds:
            preds_list.extend(fold_pred)
        for fold_res in token_metrics:
            metrics_list.extend(fold_res)

    # Save predictions and metrics
    pd.DataFrame(preds_list).to_csv("lgb_extended_preds.csv", index=False)
    pd.DataFrame(metrics_list).to_csv("lgb_extended_pinball.csv", index=False)

    # Quick summary of mean pinball loss per quantile
    met = (
        pd.DataFrame(metrics_list)
          .groupby("tau")["pinball"].mean()
          .sort_index()
    )
    print("\n=== Mean pinball-loss (Extended LGBM) ===")
    print(met.round(6))

    # Compute empirical coverage for the 80 % interval
    # Pivot predictions so each quantile is a column
    pr = pd.DataFrame(preds_list)
    piv = pr.pivot(index=["timestamp", "token"], columns="tau", values="y_pred")
    # Ensure we have q0.10 and q0.90 columns
    lower_col = 0.10
    upper_col = 0.90
    joint = (
        df.set_index(["timestamp", "token"])[[target]]
          .rename(columns={target: "y"})
          .join(piv[[lower_col, upper_col]], how="inner")
    )
    inside = ((joint["y"] >= joint[lower_col]) & (joint["y"] <= joint[upper_col])).mean()
    print(f"Empirical {int(cover * 100)} % coverage : {inside * 100:.2f} %")


if __name__ == "__main__":
    # Demonstration: This block is not executed by default.  To use
    # this script, import run_lightgbm_extended into your analysis
    # notebook or Python script, and call it with appropriate
    # arguments.
    pass
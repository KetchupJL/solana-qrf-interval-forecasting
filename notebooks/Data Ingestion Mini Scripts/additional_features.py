import pandas as pd
from hmmlearn.hmm import GaussianHMM

def compute_additional_features(df: pd.DataFrame) -> pd.DataFrame:
    """Add extreme return flags, downside volatility and HMM regime features."""
    df = df.copy()

    # 1. Extreme-Return Flags
    df['sigma_14'] = (
        df.groupby('token')['logret_12h']
          .transform(lambda x: x.rolling(14).std())
    )
    df['extreme_flag'] = (df['logret_12h'].abs() > 2 * df['sigma_14']).astype(int)
    df['extreme_count_72h'] = (
        df.groupby('token')['extreme_flag']
          .transform(lambda x: x.rolling(6).sum())
    )

    # 2. Downside Volatility
    def downside_std(x: pd.Series, window: int) -> pd.Series:
        neg = x[x < 0]
        return neg.rolling(window).std()

    for w in (3, 6):
        col = f'downside_vol_{w}bar'
        df[col] = (
            df.groupby('token')['logret_12h']
              .transform(lambda x, w=w: downside_std(x, window=w))
        )

    # 3. HMM Regime
    def fit_hmm_states(series: pd.Series, n_states: int = 2) -> pd.Series:
        series = series.dropna()
        if series.empty:
            return pd.Series(index=series.index, dtype="int64")
        X = series.values.reshape(-1, 1)
        model = GaussianHMM(
            n_components=n_states, covariance_type="diag", n_iter=100
        )
        model.fit(X)
        states = pd.Series(model.predict(X), index=series.index)
        return states

    df['hmm_regime'] = (
        df.groupby('token')['logret_12h']
          .apply(lambda g: fit_hmm_states(g))
          .reset_index(level=0, drop=True)
          .astype('Int64')
    )

    return df

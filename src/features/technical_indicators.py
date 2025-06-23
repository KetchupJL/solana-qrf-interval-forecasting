import pandas as pd
import numpy as np


def compute_technical_indicators(df: pd.DataFrame) -> pd.DataFrame:
"""Compute a suite of technical indicators.

This helper generates a range of indicators used in the forecasting
notebooks.  The following columns are added (when the required inputs are
present):

* ``stoch_k`` – Stochastic Oscillator %K calculated from 14‑period highs/lows.
* ``williams_r`` – Williams %R using the same 14‑period window.
* ``macd`` / ``macd_signal`` – 12–26 EMA difference and its 9‑period signal
  line.
* ``proc`` – price rate of change.
* ``bollinger_b`` / ``bollinger_bw`` – Bollinger %b and band width from a
  20‑period moving average.
* ``adx`` – Average Directional Index measuring trend strength.
* ``cci`` – Commodity Channel Index.
* ``obv`` – On‑Balance Volume.
* ``vol_zscore_14`` – volume Z‑score over a 14‑bar lookback.
* ``momentum_3bar`` / ``momentum_6bar`` – short‑term returns.
* ``vol_std_3bar`` / ``vol_std_7bar`` – realised volatility of log returns.
* ``holder_growth_7d`` / ``new_addr_growth_7d`` – one‑week growth in holders
  and new addresses.
* ``tvl_change_7d`` – weekly percentage change in TVL.

The function is tolerant of slightly different input column names (e.g.
``token_close_usd`` vs ``close_usd``) and operates independently on each token
in the DataFrame.
"""
    df = df.copy()

    # Ensure timestamp sorted within each token
    df = df.sort_values(["token", "timestamp"])

    high_col = "high_usd" if "high_usd" in df.columns else "token_high_usd"
    low_col = "low_usd" if "low_usd" in df.columns else "token_low_usd"
    close_col = "close_usd" if "close_usd" in df.columns else "token_close_usd"
    vol_col = "volume_usd" if "volume_usd" in df.columns else "token_volume_usd"

    high = df.groupby("token")[high_col]
    low = df.groupby("token")[low_col]
    close = df.groupby("token")[close_col]

    # 14-period highs/lows for Stochastic Oscillator and Williams %R
    highest_14 = high.transform(lambda x: x.rolling(window=14).max())
    lowest_14 = low.transform(lambda x: x.rolling(window=14).min())

    stoch_k = 100 * (df[close_col] - lowest_14) / (highest_14 - lowest_14)
    df['stoch_k'] = stoch_k

    williams_r = -100 * (highest_14 - df[close_col]) / (highest_14 - lowest_14)
    df['williams_r'] = williams_r

    # MACD using closing price
    ema12 = close.transform(lambda x: x.ewm(span=12, adjust=False).mean())
    ema26 = close.transform(lambda x: x.ewm(span=26, adjust=False).mean())
    macd = ema12 - ema26
    df['macd'] = macd
    df['macd_signal'] = macd.groupby(df['token']).transform(lambda x: x.ewm(span=9, adjust=False).mean())

    # Price Rate of Change (12 periods ~ 6 days)
    proc = close.transform(lambda x: x.pct_change(periods=12))
    df['proc'] = proc

    # Bollinger Bands (20-period)
    ma20 = close.transform(lambda x: x.rolling(window=20).mean())
    std20 = close.transform(lambda x: x.rolling(window=20).std())
    upper = ma20 + 2 * std20
    lower = ma20 - 2 * std20
    df['bollinger_b'] = (df[close_col] - lower) / (upper - lower)
    df['bollinger_bw'] = (upper - lower) / ma20

    # Average Directional Index (14-period)
    up_move = high.diff()
    down_move = (-low.diff())
    plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
    minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
    prev_close = close.shift()
    tr = pd.concat([
        df[high_col] - df[low_col],
        (df[high_col] - prev_close).abs(),
        (df[low_col] - prev_close).abs(),
    ], axis=1).max(axis=1)
    atr = tr.groupby(df['token']).transform(lambda x: x.ewm(alpha=1/14, adjust=False).mean())
    plus_di = 100 * pd.Series(plus_dm, index=df.index).groupby(df['token']).transform(lambda x: x.ewm(alpha=1/14, adjust=False).mean()) / atr
    minus_di = 100 * pd.Series(minus_dm, index=df.index).groupby(df['token']).transform(lambda x: x.ewm(alpha=1/14, adjust=False).mean()) / atr
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
    df['adx'] = dx.groupby(df['token']).transform(lambda x: x.ewm(alpha=1/14, adjust=False).mean())

    # Commodity Channel Index (20-period)
    tp = (df[high_col] + df[low_col] + df[close_col]) / 3
    tp_ma = tp.groupby(df['token']).transform(lambda x: x.rolling(window=20).mean())
    mad = tp.groupby(df['token']).transform(lambda x: x.rolling(window=20).apply(lambda y: np.mean(np.abs(y - y.mean())), raw=False))
    df['cci'] = (tp - tp_ma) / (0.015 * mad)

    # On-Balance Volume
    price_change_sign = np.sign(df[close_col] - prev_close).fillna(0)
    df['obv'] = (price_change_sign * df[vol_col]).groupby(df['token']).cumsum()

    # Volume Z-score over 14 bars
    vol = df.groupby('token')[vol_col]
    vol_mean = vol.transform(lambda x: x.rolling(window=14).mean())
    vol_std = vol.transform(lambda x: x.rolling(window=14).std())
    df['vol_zscore_14'] = (df[vol_col] - vol_mean) / vol_std

    # Short-term momentum (returns)
    df['momentum_3bar'] = close.transform(lambda x: x.pct_change(periods=3))
    df['momentum_6bar'] = close.transform(lambda x: x.pct_change(periods=6))

    # Lagged volatility (standard deviation of log returns)
    logret = close.transform(lambda x: np.log(x) - np.log(x.shift(1)))
    df['vol_std_3bar'] = logret.groupby(df['token']).transform(lambda x: x.rolling(window=3).std())
    df['vol_std_7bar'] = logret.groupby(df['token']).transform(lambda x: x.rolling(window=7).std())

    # Network activity growth metrics
    holders = df.groupby('token')['holder_count']
    df['holder_growth_7d'] = holders.transform(lambda x: x.pct_change(periods=14))

    new_addr = df.groupby('token')['new_token_accounts']
    df['new_addr_growth_7d'] = new_addr.transform(lambda x: x.pct_change(periods=14))

    tvl_col = "tvl_usd" if "tvl_usd" in df.columns else "tvl_tvl_usd"
    tvl = df.groupby("token")[tvl_col]
    df["tvl_change_7d"] = tvl.transform(lambda x: x.pct_change(periods=14))

    return df

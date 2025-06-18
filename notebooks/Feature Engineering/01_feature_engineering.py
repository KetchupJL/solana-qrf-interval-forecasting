# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.2
# ---

# %%

import pandas as pd
import numpy as np


def rsi(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    return 100 - 100 / (1 + rs)
def atr(high, low, close, period=14):
    high_low = high - low
    high_close = (high - close.shift()).abs()
    low_close = (low - close.shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period, min_periods=period).mean()



# %%

df = pd.read_parquet('data/06data.parquet')
df['timestamp'] = pd.to_datetime(df['timestamp'])
df.sort_values(['token', 'timestamp'], inplace=True)


# %%

def compute_base_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    new_cols = [
        'logret_12h', 'logret_36h', 'rsi_14', 'roc_3', 'realized_vol_36h',
        'atr_14', 'spread', 'depth', 'vol_spike', 'delta_wallets',
        'tx_count_12h', 'ret_SOL', 'ret_BTC', 'ret_ETH', 'tvl_dev'
    ]
    df.drop(columns=[c for c in new_cols if c in df.columns], inplace=True, errors='ignore')

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(['token', 'timestamp'], inplace=True)
    g = df.groupby('token')
    volume = df.get('token_volume_usd', df.get('volume'))

    df['logret_12h'] = g['token_close_usd'].transform(lambda x: np.log(x / x.shift(1)))
    df['logret_36h'] = g['token_close_usd'].transform(lambda x: np.log(x / x.shift(3)))
    df['rsi_14'] = g['token_close_usd'].transform(lambda x: rsi(x, 14))
    df['roc_3'] = g['token_close_usd'].transform(lambda x: (x / x.shift(3) - 1) * 100)
    df['realized_vol_36h'] = g['logret_12h'].transform(lambda x: x.rolling(3).std())
    df['atr_14'] = df.groupby('token', group_keys=False).apply(lambda grp: atr(grp.get('high_usd', grp.get('high')), grp.get('low_usd', grp.get('low')), grp['token_close_usd'], 14))

    if {'best_ask', 'best_bid'}.issubset(df.columns):
        mid = (df['best_ask'] + df['best_bid']) / 2
        df['spread'] = (df['best_ask'] - df['best_bid']) / mid
    else:
        df['spread'] = np.nan

    if {'bid_size', 'ask_size'}.issubset(df.columns):
        df['depth'] = df['bid_size'] + df['ask_size']
    else:
        df['depth'] = np.nan

    if volume is not None:
        df['vol_spike'] = g[volume.name].transform(lambda x: x / x.rolling(14).mean())
    else:
        df['vol_spike'] = np.nan

    uniq_wallets = df.get('unique_wallets', df.get('holder_count'))
    if uniq_wallets is not None:
        df['delta_wallets'] = g[uniq_wallets.name].transform(lambda x: x.diff())
    else:
        df['delta_wallets'] = np.nan

    df['tx_count_12h'] = df.get('tx_count', df.get('network_tx_count'))

    if 'sol_close_usd' in df.columns:
        df['ret_SOL'] = df['sol_close_usd'].pct_change() * 100
    if 'btc_close_usd' in df.columns:
        df['ret_BTC'] = df['btc_close_usd'].pct_change() * 100
    if 'eth_close_usd' in df.columns:
        df['ret_ETH'] = df['eth_close_usd'].pct_change() * 100
    if 'tvl_usd' in df.columns:
        df['tvl_dev'] = (df['tvl_usd'] / df['tvl_usd'].rolling(14).mean() - 1) * 100

    return df



# %%

df = compute_base_features(df)
df.head()


# %% [markdown]
# ## Base Feature Overview
# The `compute_base_features` function adds the following fields:
# - **logret_12h** – log return of the close over the last 12 hours.
# - **logret_36h** – log return over the previous three 12‑hour bars.
# - **rsi_14** – 14‑period Relative Strength Index using closing prices.
# - **roc_3** – 3‑period Rate of Change of the close (percent).
# - **realized_vol_36h** – rolling 36‑hour standard deviation of `logret_12h`.
# - **atr_14** – 14‑period Average True Range.
# - **spread** – relative bid/ask spread.
# - **depth** – combined bid and ask size.
# - **vol_spike** – ratio of volume to its 14‑period average.
# - **delta_wallets** – change in unique wallet count.
# - **tx_count_12h** – transaction count for the bar.
# - **ret_SOL** – SOL percentage return.
# - **ret_BTC** – BTC percentage return.
# - **ret_ETH** – ETH percentage return.
# - **tvl_dev** – deviation of DeFi TVL from its 14‑period mean.
#

# %% [markdown]
# Advanced Features

# %%

def compute_advanced_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    new_cols = [
        'parkinson_vol_36h', 'gk_vol_36h', 'amihud_illiq_12h',
        'new_accounts_ratio', 'tx_per_account', 'wallet_growth_rate',
        'corr_SOL_36h', 'corr_BTC_36h', 'corr_ETH_36h', 'vol_zscore',
        'day_of_week', 'hour'
    ]
    df.drop(columns=[c for c in new_cols if c in df.columns], inplace=True, errors='ignore')

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(['token', 'timestamp'], inplace=True)
    g = df.groupby('token')

    high = df.get('high_usd', df.get('high'))
    low = df.get('low_usd', df.get('low'))
    open_ = df.get('open_usd', df.get('open'))
    volume = df.get('token_volume_usd', df.get('volume'))

    df['parkinson_vol_36h'] = g.apply(lambda grp: (np.log(grp[high.name] / grp[low.name]) ** 2).rolling(3).mean().div(4 * np.log(2)).pow(0.5)).reset_index(level=0, drop=True)

    df['gk_vol_36h'] = g.apply(lambda grp: (0.5 * np.log(grp[high.name] / grp[low.name]) ** 2 - (2 * np.log(2) - 1) * np.log(grp['token_close_usd'] / grp[open_.name]) ** 2).rolling(3).mean().pow(0.5)).reset_index(level=0, drop=True)

    if volume is not None:
        df['amihud_illiq_12h'] = g.apply(lambda grp: (grp['logret_12h'].abs() / grp[volume.name]).rolling(3).mean()).reset_index(level=0, drop=True)
    else:
        df['amihud_illiq_12h'] = np.nan

    if {'new_token_accounts', 'holder_count'}.issubset(df.columns):
        df['new_accounts_ratio'] = df['new_token_accounts'] / df['holder_count']

    if {'network_tx_count', 'holder_count'}.issubset(df.columns):
        df['tx_per_account'] = df['network_tx_count'] / df['holder_count']

    if 'delta_wallets' in df.columns and 'holder_count' in df.columns:
        df['wallet_growth_rate'] = df['delta_wallets'] / df['holder_count']

    if {'ret_SOL', 'logret_12h'}.issubset(df.columns):
        df['corr_SOL_36h'] = g.apply(lambda grp: grp['logret_12h'].rolling(3).corr(grp['ret_SOL'])).reset_index(level=0, drop=True)
    if {'ret_BTC', 'logret_12h'}.issubset(df.columns):
        df['corr_BTC_36h'] = g.apply(lambda grp: grp['logret_12h'].rolling(3).corr(grp['ret_BTC'])).reset_index(level=0, drop=True)
    if {'ret_ETH', 'logret_12h'}.issubset(df.columns):
        df['corr_ETH_36h'] = g.apply(lambda grp: grp['logret_12h'].rolling(3).corr(grp['ret_ETH'])).reset_index(level=0, drop=True)

    if volume is not None:
        df['vol_zscore'] = g[volume.name].transform(lambda x: (x - x.rolling(14).mean()) / x.rolling(14).std())

    df['day_of_week'] = df['timestamp'].dt.dayofweek
    df['hour'] = df['timestamp'].dt.hour

    return df



# %% [markdown]
# ## Advanced Feature Overview
# The `compute_advanced_features` function augments the dataset with:
# - **parkinson_vol_36h** – 36‑hour Parkinson volatility using high and low prices.
# - **gk_vol_36h** – 36‑hour Garman‑Klass volatility from OHLC bars.
# - **amihud_illiq_12h** – Amihud illiquidity over the last 36 hours.
# - **new_accounts_ratio** – new token accounts relative to current holders.
# - **tx_per_account** – network transactions per holder.
# - **wallet_growth_rate** – change in wallet count scaled by total holders.
# - **corr_SOL_36h** – rolling correlation of token and SOL returns.
# - **corr_BTC_36h** – rolling correlation of token and BTC returns.
# - **corr_ETH_36h** – rolling correlation of token and ETH returns.
# - **vol_zscore** – volume z‑score versus a 14‑period mean and std.
# - **day_of_week** – day of week (0=Monday).
# - **hour** – bar hour of day (0 or 12).
#

# %% [markdown]
# ## Additional Feature Overview
# The `compute_additional_features` function adds:
# - **skew_36h** – rolling 36‑hour skewness of `logret_12h`.
# - **kurt_36h** – rolling 36‑hour kurtosis of `logret_12h`.
# - **vol_regime** – 1 if short volatility exceeds its 14‑period mean.
# - **trend_regime** – 1 if close is above its 50‑period average.
# - **price_volume** – close multiplied by volume.
# - **spread_vol** – bid/ask spread times `vol_spike`.
# - **market_pc1** – first principal component of SOL/BTC/ETH returns.
# - **momentum_bucket** – quantile bin of 3‑period ROC.
# - **volume_missing** – flag for missing volume.
#

# %%

def compute_additional_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    new_cols = [
        'skew_36h', 'kurt_36h', 'vol_regime', 'trend_regime', 'price_volume',
        'spread_vol', 'market_pc1', 'momentum_bucket', 'volume_missing'
    ]
    df.drop(columns=[c for c in new_cols if c in df.columns], inplace=True, errors='ignore')

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(['token', 'timestamp'], inplace=True)
    g = df.groupby('token')
    df['skew_36h'] = g['logret_12h'].transform(lambda x: x.rolling(3).skew())
    df['kurt_36h'] = g['logret_12h'].transform(lambda x: x.rolling(3).kurt())
    df['vol_regime'] = g['realized_vol_36h'].transform(lambda x: (x > x.rolling(14).mean()).astype(int))
    df['trend_regime'] = g['token_close_usd'].transform(lambda x: (x > x.rolling(50).mean()).astype(int))
    # Interaction terms
    if 'token_volume_usd' in df.columns:
        df['price_volume'] = df['token_close_usd'] * df['token_volume_usd']
    if {'spread', 'vol_spike'}.issubset(df.columns):
        df['spread_vol'] = df['spread'] * df['vol_spike']
    # Rolling PCA first component of SOL/BTC/ETH returns
    if {'ret_SOL','ret_BTC','ret_ETH'}.issubset(df.columns):
        from sklearn.decomposition import PCA
        rets = df[['ret_SOL','ret_BTC','ret_ETH']]
        pc1 = [np.nan] * len(rets)
        pca = PCA(n_components=1)
        for i in range(13, len(rets)):
            window = rets.iloc[i-13:i+1].dropna()
            if len(window) == 14:
                pca.fit(window)
                pc1[i] = pca.transform(rets.iloc[[i]])[0,0]
        df['market_pc1'] = pc1
    # Momentum quantile bins
    if 'roc_3' in df.columns:
        df['momentum_bucket'] = pd.qcut(df['roc_3'].rank(method='first'), q=5, labels=False)
    # Missing data flag
    if 'token_volume_usd' in df.columns:
        df['volume_missing'] = df['token_volume_usd'].isna().astype(int)
    return df



# %%

# compute base and advanced features

df = compute_base_features(df)
df = compute_advanced_features(df)
df = compute_additional_features(df)
df.head()


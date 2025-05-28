# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_btc_eth_defi.py"

#!/usr/bin/env python3
"""
Backfill 6-month history of BTC & ETH 12h OHLC and global DeFi TVL change.

Outputs:
  - btc_eth_price.csv : timestamp, btc_open, btc_high, btc_low, btc_close,
                        eth_open, eth_high, eth_low, eth_close
  - defi_tvl.csv      : timestamp, defi_tvl_usd, defi_tvl_change_12h

Uses CoinGecko public API (no key) and DefiLlama `/charts/{chain}` for global DeFi.
Splits price requests into ≤90-day chunks to avoid daily-only data.
"""
import time
import requests
import pandas as pd
import os

os.makedirs("data", exist_ok=True)

# ── Config ─────────────────────────────────────────────────────────
COINGECKO    = "https://api.coingecko.com/api/v3"
DEFI_CHART   = "https://api.llama.fi/charts/defi"
BIN_FREQ     = '12h'
WINDOW_DAYS  = 180  # six months
CHUNK_DAYS   = 90   # max range for finer-than-daily granularity
RETRY_DELAY  = 2    # seconds between retries
MAX_RETRIES  = 5

# ── Helpers ────────────────────────────────────────────────────────
def fetch_price_range(coin_id, vs_currency, start, end):
    """
    Fetch ms timestamps and prices for a coin via CoinGecko market_chart/range,
    in CHUNK_DAYS-sized windows to preserve >12h detail.
    Returns a pd.Series indexed by timestamp.
    """
    all_records = []
    window = CHUNK_DAYS * 24 * 3600
    cur = start
    while cur < end:
        to_ts = min(cur + window, end)
        for attempt in range(MAX_RETRIES):
            try:
                r = requests.get(
                    f"{COINGECKO}/coins/{coin_id}/market_chart/range",
                    params={'vs_currency': vs_currency, 'from': cur, 'to': to_ts},
                    timeout=30
                )
                r.raise_for_status()
                data = r.json().get('prices', [])
                all_records.extend(data)
                break
            except Exception:
                time.sleep(RETRY_DELAY * (2 ** attempt))
        cur = to_ts
        time.sleep(1)

    # build DataFrame, drop duplicates
    df = pd.DataFrame(all_records, columns=['ms','price_usd'])
    df = df.drop_duplicates(subset=['ms']).dropna()
    df['timestamp'] = pd.to_datetime(df['ms'], unit='ms', errors='coerce')
    return df.set_index('timestamp')['price_usd']

# ── Main ─────────────────────────────────────────────────────────
def main():
    now = int(time.time())
    start = now - WINDOW_DAYS * 24 * 3600

    # 1) BTC & ETH prices
    print("Fetching BTC price in 90-day chunks...")
    btc_series = fetch_price_range('bitcoin', 'usd', start, now)
    print("Fetching ETH price in 90-day chunks...")
    eth_series = fetch_price_range('ethereum', 'usd', start, now)

    # Resample to 12h OHLC
    btc_ohlc = btc_series.resample(BIN_FREQ).ohlc()
    eth_ohlc = eth_series.resample(BIN_FREQ).ohlc()

    # Merge and save
    df_price = pd.concat([
        btc_ohlc.add_prefix('btc_'),
        eth_ohlc.add_prefix('eth_')
    ], axis=1)
    df_price.dropna(how='all', inplace=True)
    df_price.to_csv('btc_eth_price.csv')
    print(f"Saved btc_eth_price.csv with {len(df_price)} rows")

    # 2) Global DeFi TVL by summing all chains
    print("Fetching global DeFi TVL by summing all chains (/charts)...")
    r = requests.get(DEFI_CHART, timeout=60)
    r.raise_for_status()
    charts = r.json()

    # Accumulate TVL across all chains per timestamp
    sum_map = {}
    for entry in charts:
        tvl_list = entry.get('tvl', [])
        for ms, tvl in tvl_list:
            if tvl is None:
                continue
            sum_map[ms] = sum_map.get(ms, 0.0) + tvl

    # Build DataFrame
    df_tvl = pd.DataFrame(list(sum_map.items()), columns=['ms','defi_tvl_usd'])
    df_tvl['timestamp'] = pd.to_datetime(pd.to_numeric(df_tvl['ms'], errors='coerce'), unit='ms', errors='coerce')
    df_tvl = df_tvl.dropna(subset=['timestamp']).set_index('timestamp').sort_index()
    if df_tvl.empty:
        print("Error: no valid DefiLlama /charts data after parsing.")
        return

    # Resample, forward-fill, pct change
    defi_12h = df_tvl['defi_tvl_usd'].resample(BIN_FREQ).last().ffill()
    defi_change = defi_12h.pct_change().fillna(0)
    out = pd.DataFrame({
        'timestamp':            defi_12h.index,
        'defi_tvl_usd':         defi_12h.values,
        'defi_tvl_change_12h':  defi_change.values
    })
    # Trim to last WINDOW_DAYS
    cutoff = pd.Timestamp.utcnow().tz_localize(None) - pd.Timedelta(days=WINDOW_DAYS)
    out = out[out['timestamp'] >= cutoff].reset_index(drop=True)

    out.to_csv('data/defi_tvl.csv', index=False)
    print(f"Saved defi_tvl.csv with {len(out)} rows")
    print(out.head(), "...", out.tail(), sep="")

if __name__ == '__main__':
    main()
    main()

#python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_tvl.py"

#!/usr/bin/env python3
"""
Backfill Solana TVL (DefiLlama) for the last 6 months and resample into 12-hour bins.
Handles varied JSON structures, cleans NaNs, forward-fills missing TVL, and trims to 6 months.
"""

import requests
import pandas as pd
import os

os.makedirs("data", exist_ok=True)
# ── Configuration ─────────────────────────────────────────────
TVL_URL     = "https://api.llama.fi/charts/solana"
BIN_FREQ    = '12h'
WINDOW_DAYS = 180  # six months

# ── Fetch & Parse ─────────────────────────────────────────────
def fetch_tvl_raw():
    resp = requests.get(TVL_URL, timeout=30)
    resp.raise_for_status()
    return resp.json()

# ── Main ───────────────────────────────────────────────────────
def main():
    raw = fetch_tvl_raw()

    # Handle dict wrapper
    if isinstance(raw, dict):
        if 'chart' in raw and isinstance(raw['chart'], list):
            raw = raw['chart']
        elif 'data' in raw and isinstance(raw['data'], list):
            raw = raw['data']
        else:
            print("Unexpected JSON structure, keys:", raw.keys())
            return

    # Validate list
    if not isinstance(raw, list) or len(raw) == 0:
        print("Error: TVL API returned empty or non-list response.")
        return

    # Build DataFrame
    df = pd.DataFrame(raw)
    if df.shape[1] >= 2:
        df = df.iloc[:, :2]
        df.columns = ['ts', 'tvl_usd']
    else:
        print("Error: TVL data format unexpected; each row must have two elements.")
        return

    # Clean and parse timestamps
    df = df.dropna(subset=['ts', 'tvl_usd'])
    df['timestamp'] = pd.to_datetime(pd.to_numeric(df['ts'], errors='coerce'), unit='s', errors='coerce')
    df = df.dropna(subset=['timestamp']).set_index('timestamp')
    df.sort_index(inplace=True)
    if df.empty:
        print("Error: no valid TVL data after parsing timestamps.")
        return

    # Resample to 12h and forward-fill missing
    tvl_12h = df['tvl_usd'].resample(BIN_FREQ).last().ffill()
    if tvl_12h.empty:
        print("Error: no TVL data after resampling.")
        return
    tvl_change = tvl_12h.pct_change().fillna(0)

    # Combine
    out = pd.DataFrame({
        'timestamp':      tvl_12h.index,
        'tvl_usd':        tvl_12h.values,
        'tvl_change_12h': tvl_change.values
    })
    
    # Trim to last WINDOW_DAYS days
    cutoff = pd.Timestamp.utcnow()
    # ensure cutoff is timezone-naive to match timestamp index
    cutoff = cutoff.tz_localize(None) if cutoff.tzinfo else cutoff
    out = out[out['timestamp'] >= cutoff - pd.Timedelta(days=WINDOW_DAYS)].reset_index(drop=True)

    # Save
    print(f"Saved tvl.csv with {len(out)} rows (last {WINDOW_DAYS} days)")
    print(out.head(), "\n…\n", out.tail())
    out.to_csv('data/tvl.csv', index=False)

if __name__ == '__main__':
    main()

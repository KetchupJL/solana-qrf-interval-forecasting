# .\venv\Scripts\Activate.ps1
# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_sol_price.py"

#!/usr/bin/env python3
"""
Backfill 6-month history of Solana USD price, aggregated into 12-hour OHLCV.

Outputs:
  - data/sol_price.csv : timestamp, open, high, low, close, volume

Uses CoinGecko free `/market_chart/range` endpoint in 30-day slices with retry logic.
"""
import time
import requests
import pandas as pd
import os

# Ensure data directory exists
os.makedirs("data", exist_ok=True)

# ── Configuration ──────────────────────────────────────────────────
COINGECKO  = "https://api.coingecko.com/api/v3"
CHUNK_SEC   = 30 * 24 * 3600   # 30-day slices
CHUNK_DELAY = 10               # wait 10 s between chunks
RETRIES     = 5
BIN_FREQ    = '12H'
WINDOW_SEC  = 180 * 24 * 3600  # last 180 days

# ── Helpers ───────────────────────────────────────────────────────
def safe_get(url, params):
    for i in range(RETRIES):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429:
            wait = 2 ** i
            print(f"429 from CoinGecko, sleeping {wait}s…")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError(f"CoinGecko failed after {RETRIES} retries for {url}")

# ── Fetch SOL price series ─────────────────────────────────────────
def fetch_sol(start, end):
    parts = []
    for s in range(start, end, CHUNK_SEC):
        t = min(s + CHUNK_SEC, end)
        print(f"Fetching SOL price from {time.strftime('%Y-%m-%d', time.gmtime(s))} to {time.strftime('%Y-%m-%d', time.gmtime(t))}...")
        data = safe_get(
            f"{COINGECKO}/coins/solana/market_chart/range",
            {'vs_currency': 'usd', 'from': s, 'to': t}
        ).get('prices', [])
        if data:
            df = pd.DataFrame(data, columns=['ms', 'price_usd'])
            df['timestamp'] = pd.to_datetime(df['ms'], unit='ms', errors='coerce')
            parts.append(df[['timestamp', 'price_usd']])
        time.sleep(CHUNK_DELAY)
    all_df = pd.concat(parts, ignore_index=True).drop_duplicates('timestamp')
    return all_df.set_index('timestamp')

# ── Main ─────────────────────────────────────────────────────────
def main():
    now = int(time.time())
    start = now - WINDOW_SEC
    df = fetch_sol(start, now)

    # Compute 12h OHLC and volume
    ohlc = df['price_usd'].resample(BIN_FREQ).ohlc()
    vol = df['price_usd'].resample(BIN_FREQ).sum()
    ohlc['volume'] = vol

    output_path = 'data/sol_price.csv'
    ohlc.to_csv(output_path, index=True)
    print(f"Saved {output_path} with {len(ohlc)} rows")
    print(ohlc.head(), "\n…\n", ohlc.tail(), sep="\n")

if __name__ == '__main__':
    main()

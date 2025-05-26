#!/usr/bin/env python3
"""
.\venv\Scripts\Activate.ps1
python .\src\ingest\solana_data_ingest.py
#!/usr/bin/env python3

Simple CoinGecko-based Solana Price Ingest + Optional Holder Counts

1. Reads `Token List - Sheet1.csv` (mint,name).
2. Fetches USD prices from CoinGecko contract API in 90-day chunks:
     GET https://api.coingecko.com/api/v3/coins/solana/contract/{mint}/market_chart/range
       ?vs_currency=usd&from=<unix>&to=<unix>
3. Concatenates, dedupes, and resamples to 12 h OHLC.
4. (Optional) GET SolanaTracker /holders/chart/{mint}?type=12h for holder counts.
5. Appends into `data/solana_data_12h.db` (table: ohlcv_12h) and `data/solana_data_12h.parquet`.
"""

import os
import time
import sqlite3
import requests
import pandas as pd
from requests.exceptions import HTTPError, RequestException

# Configuration
TOKENS_CSV    = 'docs/Token List - Sheet1.csv'
OUTPUT_DIR      = 'data'
SQLITE_DB       = os.path.join(OUTPUT_DIR, 'solana_data_12h.db')
PARQUET_FILE    = os.path.join(OUTPUT_DIR, 'solana_data_12h.parquet')
USE_PARQUET     = False   # set True to export Parquet
COINGECKO_API   = 'https://api.coingecko.com/api/v3'
SOL_TRK_KEY     = os.getenv('SOLANATRACKER_API_KEY')
# 7 days in seconds for chunk size
CHUNK_SEC       = 7 * 24 * 3600
# backoff settings for CoinGecko
CG_MAX_RETRIES    = 5
CG_BACKOFF_FACTOR = 2
# throttle between chunk requests (in seconds)
CHUNK_DELAY      = 10
# throttle between tokens (in seconds)
TOKEN_DELAY      = 5

os.makedirs(OUTPUT_DIR, exist_ok=True)

# Load tokens
def load_tokens(path):
    df = pd.read_csv(path, header=None, names=['mint','name'], dtype=str)
    return df.dropna(subset=['mint']).reset_index(drop=True)

# Safe GET for CoinGecko with retry/backoff on 429s and network errors
def safe_get_coingecko(url, params):
    for attempt in range(CG_MAX_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 429:
                wait = CG_BACKOFF_FACTOR * (2 ** attempt)
                print(f"429 from CoinGecko, retrying in {wait}s…")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except RequestException as e:
            if attempt < CG_MAX_RETRIES - 1:
                wait = CG_BACKOFF_FACTOR * (2 ** attempt)
                print(f"Error {e} from CoinGecko, retrying in {wait}s…")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"CoinGecko GET {url} failed after {CG_MAX_RETRIES} attempts")

# Fetch price series from CoinGecko
def fetch_price_usd(mint, start, end):
    dfs = []
    for s in range(start, end, CHUNK_SEC):
        t = min(s + CHUNK_SEC, end)
        url = f"{COINGECKO_API}/coins/solana/contract/{mint}/market_chart/range"
        params = {'vs_currency':'usd','from':s,'to':t}
        data = safe_get_coingecko(url, params).get('prices', [])
        if data:
            df = pd.DataFrame(data, columns=['ms','price_usd'])
            df['timestamp'] = pd.to_datetime(df['ms'], unit='ms')
            dfs.append(df[['timestamp','price_usd']])
        # throttle to avoid rate-limit
        time.sleep(CHUNK_DELAY)
    if not dfs:
        return pd.DataFrame(columns=['timestamp','price_usd'])
    all_df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=['timestamp'])
    return all_df.set_index('timestamp')

# Resample to 12h OHLC + volume
def resample_12h(df):
    ohlc = df['price_usd'].resample('12h').ohlc()
    vol  = df['price_usd'].resample('12h').sum()
    ohlc['volume'] = vol
    return ohlc.reset_index()

# Optional holder counts from SolanaTracker
def fetch_holders(mint, start, end):
    if not SOL_TRK_KEY:
        return pd.DataFrame(columns=['timestamp','holder_count'])
    url = f"https://data.solanatracker.io/holders/chart/{mint}"
    headers = {'x-api-key': SOL_TRK_KEY}
    params = {'type':'12h','time_from':start,'time_to':end}
    try:
        resp = requests.get(url, headers=headers, params=params, timeout=10)
        if resp.status_code == 404:
            return pd.DataFrame(columns=['timestamp','holder_count'])
        resp.raise_for_status()
    except HTTPError:
        return pd.DataFrame(columns=['timestamp','holder_count'])
    rec = resp.json().get('holders', [])
    df = pd.DataFrame(rec)
    if df.empty:
        return df
    df['timestamp'] = pd.to_datetime(df['time'], unit='s')
    return df.rename(columns={'holders':'holder_count'})[['timestamp','holder_count']]

# Main
def main():
    now   = int(time.time())
    start = now - 180*24*3600  # last 180 days (~6 months)

    # Initialize SQLite
    conn = sqlite3.connect(SQLITE_DB)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS ohlcv_12h (
        timestamp TEXT,
        open REAL, high REAL, low REAL, close REAL, volume REAL,
        holder_count INTEGER,
        token_mint TEXT, token_name TEXT
      )
    """
    )
    conn.commit()

    tokens = load_tokens(TOKENS_CSV)
    all_parquet = []

    for _, row in tokens.iterrows():
        mint, name = row['mint'], row['name']
        print(f"⏳ Fetching {name}…")

        # Price
        df_price = fetch_price_usd(mint, start, now)
        if df_price.empty:
            print(f"  ⚠️ No price data for {name}, skipping.")
            continue
        df_12h = resample_12h(df_price)

        # Holders
        df_h = fetch_holders(mint, start, now)

        # Merge & tag
        df = pd.merge(df_12h, df_h, on='timestamp', how='left')
        df['token_mint'] = mint
        df['token_name'] = name

        # Persist
        df.to_sql('ohlcv_12h', conn, if_exists='append', index=False)
        if USE_PARQUET:
            all_parquet.append(df)
        print(f"  ✅ Inserted {len(df)} rows for {name}")

        # throttle between tokens
        time.sleep(TOKEN_DELAY)

    if USE_PARQUET and all_parquet:
        pd.concat(all_parquet).to_parquet(PARQUET_FILE, index=False)
        print("📦 Parquet saved.")

    conn.close()
    print("🎉 Done. Data stored in", SQLITE_DB)

if __name__ == '__main__':
    main()

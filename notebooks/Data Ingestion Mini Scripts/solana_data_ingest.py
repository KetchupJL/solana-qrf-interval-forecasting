# .\venv\Scripts\Activate.ps1
# python .\src\ingest\solana_data_ingest.py

#!/usr/bin/env python3
r"""
Source: src/ingest/solana_data_ingest.py
Fetch 6 months of 12-h OHLCV for each Solana SPL token listed in docs/tokens.csv,
writing into data/solana_data_12h.db (and per-token CSVs under data/tokens/).

Holder counts are now ingested separately; this script reads holders.csv and merges.
"""
import os
import time
import sqlite3
import requests
import pandas as pd
from pathlib import Path
from requests.exceptions import RequestException

# ── Configuration ─────────────────────────────────────────────────────
# Project root is two levels up from this file (src/ingest)
ROOT          = Path(__file__).resolve().parent.parent.parent
TOKENS_CSV    = ROOT / 'docs' / 'tokens.csv'
DATA_DIR      = ROOT / 'data'
TOKENS_DIR    = DATA_DIR / 'tokens'
HOLDERS_FILE  = DATA_DIR / 'holders.csv'
DB_FILE       = DATA_DIR / 'solana_data_12h.db'
USE_PARQUET   = False
PARQUET_FILE  = DATA_DIR / 'solana_data_12h.parquet'

# CoinGecko settings
CG_API        = 'https://api.coingecko.com/api/v3'
CG_CHUNK_SEC  = 30 * 24 * 3600
CG_RETRIES    = 5
CG_BACKOFF    = 2
CG_DELAY      = 2  # delay between retries or chunks

# Pipeline settings
WINDOW_SEC    = 180 * 24 * 3600
RESAMPLE_FREQ = '12h'
DELAY_TOKEN   = 5  # seconds between tokens
DELAY_CHUNK   = 1  # seconds between CG chunks

# Ensure directories exist
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(TOKENS_DIR, exist_ok=True)

# ── Load Token List ───────────────────────────────────────────────────
def load_tokens(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Token list CSV not found at: {path}")
    df = pd.read_csv(path, header=None, names=['mint','name'], dtype=str)
    return df.dropna(subset=['mint']).reset_index(drop=True)

# ── Safe CoinGecko Fetch ──────────────────────────────────────────────
def safe_get_cg(url: str, params: dict) -> dict:
    for i in range(CG_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 404:
                print(f"  ⚠ 404 at {url}, skipping.")
                return {}
            if r.status_code == 429:
                wait = CG_BACKOFF * (2**i)
                print(f"  429 rate limit, sleeping {wait}s…")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except RequestException as e:
            wait = CG_BACKOFF * (2**i)
            print(f"  Error: {e}, retry {i+1}/{CG_RETRIES} in {wait}s…")
            time.sleep(wait)
        finally:
            time.sleep(CG_DELAY)
    print(f"  ⚠ Failed CG fetch after {CG_RETRIES} retries: {url}")
    return {}

# ── Fetch Price USD ───────────────────────────────────────────────────
def fetch_price_usd(mint: str, start: int, end: int) -> pd.DataFrame:
    all_records = []
    cur = start
    while cur < end:
        to_ts = min(cur + CG_CHUNK_SEC, end)
        url = f"{CG_API}/coins/solana/contract/{mint}/market_chart/range"
        data = safe_get_cg(url, {'vs_currency':'usd','from':cur,'to':to_ts}).get('prices', [])
        if data:
            df = pd.DataFrame(data, columns=['ms','price_usd'])
            df['timestamp'] = pd.to_datetime(df['ms'], unit='ms', errors='coerce')
            all_records.append(df[['timestamp','price_usd']])
        cur = to_ts
        time.sleep(DELAY_CHUNK)
    if not all_records:
        return pd.DataFrame(columns=['timestamp','price_usd']).set_index('timestamp')
    df_all = pd.concat(all_records, ignore_index=True).drop_duplicates('timestamp')
    return df_all.set_index('timestamp')

# ── Resample 12h ─────────────────────────────────────────────────────
def resample_12h(df_price: pd.DataFrame) -> pd.DataFrame:
    ohlc = df_price['price_usd'].resample(RESAMPLE_FREQ).ohlc()
    vol = df_price['price_usd'].resample(RESAMPLE_FREQ).sum()
    ohlc.columns = ['open_usd','high_usd','low_usd','close_usd']
    ohlc['volume_usd'] = vol
    return ohlc.reset_index()

# ── Load Holders CSV ──────────────────────────────────────────────────
def load_holders(path: Path) -> pd.DataFrame:
    if path.exists():
        df = pd.read_csv(path, parse_dates=['timestamp'])
        return df
    else:
        # no holders data yet
        return pd.DataFrame(columns=['timestamp','token_mint','holder_count'])

# ── Main Pipeline ─────────────────────────────────────────────────────
def main():
    now = int(time.time())
    start = now - WINDOW_SEC

    # prepare SQLite
    conn = sqlite3.connect(DB_FILE)
    conn.execute("DROP TABLE IF EXISTS ohlcv_12h")
    conn.execute("""
      CREATE TABLE ohlcv_12h (
        timestamp    TEXT,
        open_usd     REAL,
        high_usd     REAL,
        low_usd      REAL,
        close_usd    REAL,
        volume_usd   REAL,
        holder_count INTEGER,
        token_mint   TEXT,
        token_name   TEXT
      )""")
    conn.commit()

    tokens_df = load_tokens(TOKENS_CSV)
    holders_df = load_holders(HOLDERS_FILE)
    parquet_buf = []

    for _, row in tokens_df.iterrows():
        mint, name = row['mint'], row['name']
        print(f"⏳ {name}…")
        # price
        df_price = fetch_price_usd(mint, start, now)
        if df_price.empty:
            print(f"  ⚠ No price for {name}, skipping")
            continue
        df_12h = resample_12h(df_price)

        # holders (merge preloaded holders.csv)
        df_h = holders_df[holders_df['token_mint'] == mint][['timestamp','holder_count']]

        # ── BEGIN FIX ────────────────────────────────────────────────────
        # Make `df_h['timestamp']` timezone‐naive so it matches df_12h:
        df_h['timestamp'] = df_h['timestamp'].dt.tz_localize(None)
        # ──  END FIX  ────────────────────────────────────────────────────

        # merge
        df = pd.merge(df_12h, df_h, on='timestamp', how='left')
        df['token_mint'], df['token_name'] = mint, name

        # write SQLite
        df.to_sql('ohlcv_12h', conn, if_exists='append', index=False)

        # per-token CSV
        out_csv = TOKENS_DIR / f"{name}_{mint}_12h.csv"
        df.to_csv(out_csv, index=False)
        if USE_PARQUET:
            parquet_buf.append(df)

        print(f"  ✅ {len(df)} rows for {name}")
        time.sleep(DELAY_TOKEN)

    if USE_PARQUET and parquet_buf:
        pd.concat(parquet_buf).to_parquet(PARQUET_FILE, index=False)
    conn.close()
    print(f"🎉 Done — data in {DB_FILE}")

if __name__ == '__main__':
    main()

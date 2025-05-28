
# .\venv\Scripts\Activate.ps1
# python .\src\ingest\solana_data_ingest.py
#!/usr/bin/env python3

#!/usr/bin/env python3
r"""
Source: src/ingest/solana_data_ingest.py
Fetch 6 months of 12-h OHLCV + holder counts for each Solana SPL token listed in Token List CSV,
writing into data/solana_data_12h.db (and per-token CSVs under data/tokens/).

Features:
 - Renamed columns to avoid SQL reserved words: open_usd, high_usd, low_usd, close_usd, volume_usd
 - Safe CoinGecko fetch with 429/backoff and general retry delay
 - SolanaTracker holder fetch with retry/backoff; optional via SOLANATRACKER_API_KEY
 - Outputs to data/ and data/tokens/ folders
"""
import os
import time
import sqlite3
import requests
import pandas as pd
from pathlib import Path
from requests.exceptions import RequestException

# ── Configuration ─────────────────────────────────────────────────────
ROOT          = Path(__file__).parent.parent
TOKENS_CSV = 'docs/tokens.csv'
DATA_DIR      = ROOT / 'data'
TOKENS_DIR    = DATA_DIR / 'tokens'
DB_FILE       = DATA_DIR / 'solana_data_12h.db'
USE_PARQUET   = False
PARQUET_FILE  = DATA_DIR / 'solana_data_12h.parquet'

CG_API        = 'https://api.coingecko.com/api/v3'
CG_CHUNK_SEC  = 30 * 24 * 3600
CG_RETRIES    = 5
CG_BACKOFF    = 2
CG_DELAY      = 5  # seconds between all retry attempts to avoid rate limits
# SolanaTracker
SOL_TRACK_URL = 'https://data.solanatracker.io/holders/chart'
SOL_TRACK_KEY = os.getenv('SOLANATRACKER_API_KEY')
# Pipeline settings
WINDOW_SEC    = 180 * 24 * 3600
RESAMPLE_FREQ = '12H'
DELAY_TOKEN   = 5  # seconds between tokens
DELAY_CHUNK   = 3  # seconds between CG chunks

# ── Ensure directories exist ──────────────────────────────────────────
DATA_DIR.mkdir(exist_ok=True)
TOKENS_DIR.mkdir(exist_ok=True)

# ── Load Token List ────────────────────────────────────────────────
def load_tokens(path):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Token list CSV not found at {path}")
    df = pd.read_csv(path, header=None, names=['mint','name'], dtype=str)
    return df.dropna(subset=['mint']).reset_index(drop=True)

# ── Safe CoinGecko Fetch ──────────────────────────────────────────
def safe_get_cg(url, params):
    for i in range(CG_RETRIES):
        try:
            r = requests.get(url, params=params, timeout=30)
            if r.status_code == 404:
                print(f"  ⚠ 404 from CoinGecko on {url}, skipping price data.")
                return {}
            if r.status_code == 429:
                wait = CG_BACKOFF * (2**i)
                print(f"  429 from CG, sleeping {wait}s…")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except RequestException as e:
            wait = CG_BACKOFF * (2**i)
            print(f"  Error fetching {url}: {e}, retry {i+1}/{CG_RETRIES} in {wait}s…")
            time.sleep(wait)
        finally:
            time.sleep(CG_DELAY)
    print(f"  ⚠ CoinGecko failed after {CG_RETRIES} retries for {url}")
    return {}

# ── Fetch Price USD ────────────────────────────────────────────────
def fetch_price_usd(mint, start, end):
    records = []
    cur = start
    while cur < end:
        to_ts = min(cur + CG_CHUNK_SEC, end)
        url = f"{CG_API}/coins/solana/contract/{mint}/market_chart/range"
        data = safe_get_cg(url, {'vs_currency':'usd','from':cur,'to':to_ts}).get('prices', [])
        if data:
            df = pd.DataFrame(data, columns=['ms','price_usd'])
            df['timestamp'] = pd.to_datetime(df['ms'], unit='ms', errors='coerce')
            records.append(df[['timestamp','price_usd']])
        cur = to_ts
        time.sleep(DELAY_CHUNK)
    if not records:
        return pd.DataFrame(columns=['timestamp','price_usd']).set_index('timestamp')
    df_all = pd.concat(records, ignore_index=True).drop_duplicates('timestamp')
    return df_all.set_index('timestamp')

# ── Fetch Holders ─────────────────────────────────────────────────
def fetch_holders(mint, start, end):
    if not SOL_TRACK_KEY:
        return pd.DataFrame(columns=['timestamp','holder_count'])
    headers = {'x-api-key': SOL_TRACK_KEY}
    params = {'type':'12h','time_from':start,'time_to':end}
    for i in range(4):
        r = requests.get(f"{SOL_TRACK_URL}/{mint}", headers=headers, params=params, timeout=15)
        if r.status_code == 429:
            wait = 2**i
            print(f"    429 holders, sleeping {wait}s…")
            time.sleep(wait)
            continue
        if r.status_code == 404:
            return pd.DataFrame(columns=['timestamp','holder_count'])
        r.raise_for_status()
        data = r.json().get('holders', [])
        df = pd.DataFrame(data)
        df['timestamp'] = pd.to_datetime(df['time'], unit='s', errors='coerce')
        return df[['timestamp','holders']].rename(columns={'holders':'holder_count'})
    return pd.DataFrame(columns=['timestamp','holder_count'])

# ── Resample 12h ───────────────────────────────────────────────────
def resample_12h(df_price):
    ohlc = df_price['price_usd'].resample(RESAMPLE_FREQ).ohlc()
    vol  = df_price['price_usd'].resample(RESAMPLE_FREQ).sum()
    ohlc.columns = ['open_usd','high_usd','low_usd','close_usd']
    ohlc['volume_usd'] = vol
    return ohlc.reset_index()

# ── Main Pipeline ─────────────────────────────────────────────────
def main():
    now   = int(time.time())
    start = now - WINDOW_SEC

    conn = sqlite3.connect(DB_FILE)
    conn.execute("""
      CREATE TABLE IF NOT EXISTS ohlcv_12h (
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

    tokens = load_tokens(TOKENS_CSV)
    parquet_buf = []

    for _, row in tokens.iterrows():
        mint, name = row['mint'], row['name']
        print(f"⏳ {name}…")
        df_price = fetch_price_usd(mint, start, now)
        if df_price.empty:
            print(f"  ⚠ no price for {name}, skipping")
            continue
        df_12h = resample_12h(df_price)
        df_h   = fetch_holders(mint, start, now)
        df     = pd.merge(df_12h, df_h, on='timestamp', how='left')
        df['token_mint'], df['token_name'] = mint, name
        df.to_sql('ohlcv_12h', conn, if_exists='append', index=False)
        out_csv = TOKENS_DIR / f"{name}_{mint}_12h.csv"
        df.to_csv(out_csv, index=False)
        if USE_PARQUET:
            parquet_buf.append(df)
        print(f"  ✅ {len(df)} rows")
        time.sleep(DELAY_TOKEN)

    if USE_PARQUET and parquet_buf:
        pd.concat(parquet_buf).to_parquet(PARQUET_FILE, index=False)
    conn.close()
    print("🎉 Done — data in", DB_FILE)

if __name__=='__main__':
    main()

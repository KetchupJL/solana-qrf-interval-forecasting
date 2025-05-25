#!/usr/bin/env python3
"""
Integrated SolanaTracker Data Ingestion Script

This script backfills 12-hour OHLCV bars, holder counts, and (stubbed) transaction/wallet stats
for a configurable list of Solana tokens, saving each series as a Parquet file with token metadata.

Requirements:
  - Python 3.8+
  - pandas
  - requests
  - pyarrow (for Parquet)
"""


import os
import time
import requests
import pandas as pd

# Configuration
TOKENS_CSV = 'docs/Token List - Sheet1.csv'  # Path to your CSV of token_mint,token_name
API_KEY = os.getenv('SOLANATRACKER_API_KEY', 'YOUR_API_KEY_HERE')
BASE_URL = 'https://data.solanatracker.io'
HEADERS = {'x-api-key': API_KEY}


def load_token_list(csv_path: str) -> pd.DataFrame:
    """
    Load token mint addresses and human-readable names from CSV.
    Assumes first row may be blank or header-less, so skips if needed.
    """
    # Read with explicit column names, skip any header row if present
    df = pd.read_csv(csv_path, header=None, names=['token_mint','token_name'], dtype=str)
    # Drop rows missing a token_mint
    df = df.dropna(subset=['token_mint']).reset_index(drop=True)
    return df


def get_pools(token_mint: str) -> list:
    """
    Retrieve all pool IDs for a given token from SolanaTracker.
    """
    url = f"{BASE_URL}/tokens/{token_mint}"
    resp = requests.get(url, headers=HEADERS)
    resp.raise_for_status()
    pools = resp.json().get("pools", [])
    return [p['poolId'] for p in pools]


def fetch_ohlcv_12h(token_mint: str, pool_id: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """
    Fetch 12h OHLCV bars for a token-pool pair.
    """
    url = f"{BASE_URL}/chart/{token_mint}/{pool_id}"
    params = {'type': '12h', 'time_from': start_ts, 'time_to': end_ts}
    resp = requests.get(url, params=params, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json().get('oclhv', [])
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['time'], unit='s')
    return df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]


def fetch_holders_12h(token_mint: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """
    Fetch 12h holder counts for a token.
    """
    url = f"{BASE_URL}/holders/chart/{token_mint}"
    params = {'type': '12h', 'time_from': start_ts, 'time_to': end_ts}
    resp = requests.get(url, params=params, headers=HEADERS)
    resp.raise_for_status()
    data = resp.json().get('holders', [])
    df = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['time'], unit='s')
    df = df.rename(columns={'holders': 'holder_count'})
    return df[['timestamp', 'holder_count']]


def fetch_stats_12h(token_mint: str, pool_id: str, start_ts: int, end_ts: int) -> pd.DataFrame:
    """
    TODO: Implement transaction and wallet stats for 12h intervals once endpoint confirmed.
    """
    raise NotImplementedError("fetch_stats_12h endpoint details required")


def merge_dataframes(dfs: list) -> pd.DataFrame:
    """
    Merge multiple DataFrames on the 'timestamp' column via left joins.
    """
    df = dfs[0]
    for d in dfs[1:]:
        df = pd.merge(df, d, on='timestamp', how='left')
    return df


def main():
    # Load tokens
    token_df = load_token_list(TOKENS_CSV)

    # Time window for backfill (e.g., last 6 months)
    now_ts = int(time.time())
    six_months = 6 * 30 * 24 * 3600
    start_ts = now_ts - six_months

    for _, row in token_df.iterrows():
        token_mint = row['token_mint']
        token_name = row['token_name']
        pools = get_pools(token_mint)

        for pool_id in pools:
            print(f"Processing {token_name} ({token_mint}), pool={pool_id}")
            try:
                df_price = fetch_ohlcv_12h(token_mint, pool_id, start_ts, now_ts)
                df_holders = fetch_holders_12h(token_mint, start_ts, now_ts)
                # df_stats = fetch_stats_12h(token_mint, pool_id, start_ts, now_ts)

                # Merge and enrich
                df = merge_dataframes([df_price, df_holders])  # add df_stats when ready
                df['token_mint'] = token_mint
                df['token_name'] = token_name

                # Sort and export
                df = df.sort_values('timestamp')
                filename = f"{token_name}_{token_mint}_{pool_id}_12h.parquet"
                df.to_parquet(filename, index=False)
                print(f"Saved: {filename}")

            except Exception as e:
                print(f"Error processing {token_name} ({token_mint}), pool={pool_id}: {e}")

if __name__ == '__main__':
    main()
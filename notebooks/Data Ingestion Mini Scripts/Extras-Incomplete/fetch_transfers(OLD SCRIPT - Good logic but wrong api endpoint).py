# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_transfers.py"

#!/usr/bin/env python3
"""
fetch_transfers.py

For each SPL token mint in `tokens.csv`, page through Helius’s
GET /v0/tokens/{mint}/transactions?type=TRANSFER endpoint (newest→oldest)
and count how many transfer events happened in each 12-hour window over the
last ~180 days. Output a per-mint CSV under data/transfers/.

Requirements:
  pip install requests

Usage:
  1) Set your API key in the environment:
       export HELIUS_API_KEY="your_real_helius_key"
     (Windows PowerShell: $env:HELIUS_API_KEY = "your_real_helius_key")

  2) Ensure `tokens.csv` lives next to this script (one mint per line).

  3) Run:
       python fetch_transfers.py
"""

import os
import time
import csv
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ─── CONFIG ─────────────────────────────────────────────────────────────────

# Read your Helius API key from environment:
HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
if not HELIUS_API_KEY:
    raise RuntimeError("Error: Please set the environment variable HELIUS_API_KEY to your Helius API key.")

# Path to tokens.csv (one mint per line, no header)
TOKENS_CSV = Path("tokens.csv")

# Output directory (will create if it doesn’t exist)
OUTPUT_DIR = Path("data/transfers")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# How far back to go (in days) – about 180 days (~6 months)
WINDOW_DAYS = 180

# Each bin is 12 hours
BIN_SECONDS = 12 * 3600

# How many transfer records to request per page (max 1000 recommended by Helius)
PAGE_LIMIT = 1000

# Throttle to ≤2 enhanced-API calls/sec → we sleep ~0.6s between requests
REQUEST_PAUSE = 0.6  # seconds

# In case of rate limits or server errors, back off up to MAX_RETRIES times
MAX_RETRIES = 5
RETRY_BACKOFF = 1.5  # exponential backoff factor

# Base URL (GET) for token transactions (with {mint} placeholder)
BASE_URL = "https://api.helius.xyz/v0/tokens/{mint}/transactions"

# ─── HELIUS PAGINATION & COUNTING ────────────────────────────────────────────

def fetch_transfer_count_for_window(mint: str, window_start: int, window_end: int) -> int:
    """
    Pages through Helius’s /v0/tokens/{mint}/transactions?type=TRANSFER endpoint,
    counting how many transactions have timestamp in [window_start, window_end].
    Stops paging once we see tx.timestamp < window_start, since results come
    newest → oldest.
    """
    url = BASE_URL.format(mint=mint)
    params = {
        "api-key": HELIUS_API_KEY,
        "limit": PAGE_LIMIT,
        "type": "TRANSFER"
    }
    total_count = 0
    cursor = None
    backoff = 1.0

    while True:
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                # Rate limit hit: back off & retry
                time.sleep(backoff)
                backoff *= RETRY_BACKOFF
                continue
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            # If a transient server error (5xx), back off & retry
            status = getattr(e.response, "status_code", None)
            if status in (500, 502, 503, 504):
                time.sleep(backoff)
                backoff *= RETRY_BACKOFF
                continue
            else:
                # Otherwise, raise
                raise

        tx_list = data.get("transactions", [])
        cursor = data.get("cursor", None)

        if not tx_list:
            # No more pages to fetch
            break

        for tx in tx_list:
            tx_ts = tx.get("timestamp", 0)
            if tx_ts < window_start:
                # We’ve now reached transfers older than this window_start.
                return total_count
            if window_start <= tx_ts <= window_end:
                total_count += 1

        if not cursor:
            # Exhausted all pages
            break

        # Throttle before next page
        time.sleep(REQUEST_PAUSE)

    return total_count

# ─── MAIN: SLIDE OVER 180 DAYS IN 12H BINS ───────────────────────────────────

def main():
    # Step 1: load all mints from tokens.csv
    if not TOKENS_CSV.exists():
        raise FileNotFoundError(f"Could not find {TOKENS_CSV}. Please create a tokens.csv (one mint per line).")

    with open(TOKENS_CSV, newline="") as f:
        reader = csv.reader(f)
        mints = [row[0].strip() for row in reader if row and row[0].strip()]
    if not mints:
        raise RuntimeError(f"{TOKENS_CSV} is empty or malformed. It should list one mint per line.")

    # Step 2: compute the UNIX timestamp range (UTC) for the past WINDOW_DAYS
    now_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = int((datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)).timestamp())

    # Build a list of (window_start, window_end) for each 12h bin
    windows = []
    t = start_ts
    while t < now_ts:
        end = min(t + BIN_SECONDS, now_ts)
        windows.append((t, end))
        t = end

    # Step 3: for each mint, compute counts per window and write a CSV
    for mint in mints:
        print(f"\n🪄  Fetching transfer counts for mint: {mint}")
        rows = []
        for (ws, we) in windows:
            count = fetch_transfer_count_for_window(mint, ws, we)
            # Convert to ISO timestamps for readability
            start_iso = datetime.fromtimestamp(ws, tz=timezone.utc).isoformat()
            # We label each bin by its **start** time
            rows.append((start_iso, mint, count))
            print(f"  {start_iso} → count={count}")
            time.sleep(REQUEST_PAUSE)

        # Write CSV: data/transfers/{mint}_transfers_12h.csv
        out_path = OUTPUT_DIR / f"{mint}_transfers_12h.csv"
        with open(out_path, "w", newline="") as out_f:
            writer = csv.writer(out_f)
            writer.writerow(["timestamp", "token_mint", "transfers_count"])
            writer.writerows(rows)
        print(f"✅  Wrote {len(rows)} rows to {out_path}")

    print("\n🎉 All done! Transfer counts are under data/transfers/")

if __name__ == "__main__":
    main()

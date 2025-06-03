# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_active_wallets.py"

#!/usr/bin/env python3
"""
fetch_active_wallets.py

For each SPL token mint in `tokens.csv`, page through Helius’s
GET /v0/tokens/{mint}/transactions?type=TRANSFER endpoint (newest→oldest),
collect every `fromUserAccount` and `toUserAccount` in the 12-hour window,
and report the count of UNIQUE addresses per bin.

Requirements:
  pip install requests

Usage:
  1) export HELIUS_API_KEY="your_real_helius_key"
  2) Ensure tokens.csv exists (one mint per line).
  3) python fetch_active_wallets.py
"""

import os
import time
import csv
import requests
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ─── CONFIG ─────────────────────────────────────────────────────────────────

HELIUS_API_KEY = os.getenv("HELIUS_API_KEY")
if not HELIUS_API_KEY:
    raise RuntimeError("Error: Please set the environment variable HELIUS_API_KEY to your Helius API key.")

TOKENS_CSV = Path("tokens.csv")

OUTPUT_DIR = Path("data/active_wallets")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

WINDOW_DAYS = 180
BIN_SECONDS = 12 * 3600
PAGE_LIMIT  = 1000
REQUEST_PAUSE = 0.6
MAX_RETRIES = 5
RETRY_BACKOFF = 1.5

BASE_URL = "https://api.helius.xyz/v0/tokens/{mint}/transactions"

# ─── HELIUS PAGINATION & UNIQUE ADDRESS COUNT ────────────────────────────────

def fetch_unique_wallets_for_window(mint: str, window_start: int, window_end: int) -> int:
    """
    Page through /v0/tokens/{mint}/transactions?type=TRANSFER, collect
    all fromUserAccount/toUserAccount in that 12h window, and return
    the count of UNIQUE addresses.

    Stops paging once we encounter tx.timestamp < window_start.
    """
    url = BASE_URL.format(mint=mint)
    params = {
        "api-key": HELIUS_API_KEY,
        "limit": PAGE_LIMIT,
        "type": "TRANSFER"
    }
    unique_addresses = set()
    cursor = None
    backoff = 1.0

    while True:
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(url, params=params, timeout=20)
            if resp.status_code == 429:
                time.sleep(backoff)
                backoff *= RETRY_BACKOFF
                continue
            resp.raise_for_status()
            data = resp.json()
        except requests.RequestException as e:
            status = getattr(e.response, "status_code", None)
            if status in (500, 502, 503, 504):
                time.sleep(backoff)
                backoff *= RETRY_BACKOFF
                continue
            else:
                raise

        tx_list = data.get("transactions", [])
        cursor  = data.get("cursor", None)

        if not tx_list:
            break

        for tx in tx_list:
            tx_ts = tx.get("timestamp", 0)
            if tx_ts < window_start:
                # All future pages will be older than window_start
                return len(unique_addresses)

            if window_start <= tx_ts <= window_end:
                # Each tx has a "tokenTransfers" array; collect from/to
                for t in tx.get("tokenTransfers", []):
                    # Only count if the mint matches (just in case)
                    if t.get("mint", "").lower() == mint.lower():
                        frm = t.get("fromUserAccount")
                        to  = t.get("toUserAccount")
                        if frm:
                            unique_addresses.add(frm)
                        if to:
                            unique_addresses.add(to)

        if not cursor:
            break

        time.sleep(REQUEST_PAUSE)

    return len(unique_addresses)

# ─── MAIN: SLIDE OVER 180 DAYS IN 12H BINS ───────────────────────────────────

def main():
    # Load mints
    if not TOKENS_CSV.exists():
        raise FileNotFoundError(f"Could not find {TOKENS_CSV}. Please create a tokens.csv (one mint per line).")
    with open(TOKENS_CSV, newline="") as f:
        reader = csv.reader(f)
        mints = [row[0].strip() for row in reader if row and row[0].strip()]
    if not mints:
        raise RuntimeError(f"{TOKENS_CSV} is empty or malformed. It should list one mint per line.")

    # Compute time range
    now_ts = int(datetime.now(timezone.utc).timestamp())
    start_ts = int((datetime.now(timezone.utc) - timedelta(days=WINDOW_DAYS)).timestamp())

    # Build 12h windows
    windows = []
    t = start_ts
    while t < now_ts:
        end = min(t + BIN_SECONDS, now_ts)
        windows.append((t, end))
        t = end

    # For each mint, compute unique wallet counts per window
    for mint in mints:
        print(f"\n🪄  Fetching active wallets for mint: {mint}")
        rows = []
        for (ws, we) in windows:
            count = fetch_unique_wallets_for_window(mint, ws, we)
            start_iso = datetime.fromtimestamp(ws, tz=timezone.utc).isoformat()
            rows.append((start_iso, mint, count))
            print(f"  {start_iso} → active_wallets={count}")
            time.sleep(REQUEST_PAUSE)

        # Write CSV: data/active_wallets/{mint}_active_wallets_12h.csv
        out_path = OUTPUT_DIR / f"{mint}_active_wallets_12h.csv"
        with open(out_path, "w", newline="") as out_f:
            writer = csv.writer(out_f)
            writer.writerow(["timestamp", "token_mint", "active_wallets"])
            writer.writerows(rows)
        print(f"✅  Wrote {len(rows)} rows to {out_path}")

    print("\n🎉 All done! Active-wallet counts are under data/active_wallets/")

if __name__ == "__main__":
    main()

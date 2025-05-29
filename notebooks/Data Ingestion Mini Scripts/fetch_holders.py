# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_holders.py"

#!/usr/bin/env python3
r"""
Backfill 6-month history of SPL-token holder counts at 12-hour intervals.

Reads token list from docs/tokens.csv (or root/tokens.csv),
uses SolanaTracker’s /holders/chart/{mint} endpoint to fetch counts,
and writes out data/holders.csv:
  timestamp, token_mint, holder_count

Timestamps are formatted as ISO strings (UTC).
"""

import time
import os
import requests
import pandas as pd
from pathlib import Path
from requests.exceptions import RequestException

# ── CONFIG ─────────────────────────────────────────────────────────────
ROOT         = Path(__file__).parent.parent
# look in docs/tokens.csv first, then root/tokens.csv
CANDIDATES = [
    ROOT / "docs" / "tokens.csv",
    ROOT / "tokens.csv",
]
for p in CANDIDATES:
    if p.exists():
        TOKENS_CSV = p
        break
else:
    raise FileNotFoundError(
        "Token list not found; tried:\n  " + "\n  ".join(str(x) for x in CANDIDATES)
    )

OUTPUT_DIR   = ROOT / "data"
OUTPUT_FILE  = OUTPUT_DIR / "holders.csv"

API_BASE     = "https://data.solanatracker.io"
HOLDERS_EP   = API_BASE + "/holders/chart/{mint}"
API_KEY      = os.getenv("SOLANATRACKER_API_KEY")  # must set this!
WINDOW_DAYS  = 180
INTERVAL     = "12h"
RETRIES      = 4
BACKOFF_BASE = 2    # exponential backoff base
DELAY_REQ    = 1    # seconds between calls

# ── PREP ────────────────────────────────────────────────────────────────
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_mints(path: Path) -> list[str]:
    df = pd.read_csv(path, header=None, names=["mint","name"], dtype=str)
    return df.dropna(subset=["mint"])["mint"].tolist()


def fetch_json(url: str, params: dict, headers: dict) -> dict | None:
    for attempt in range(1, RETRIES+1):
        try:
            r = requests.get(url, params=params, headers=headers, timeout=15)
            if r.status_code == 404:
                return None
            if r.status_code == 429:
                wait = BACKOFF_BASE**(attempt-1)
                print(f"  429 rate-limit, sleeping {wait}s…")
                time.sleep(wait)
                continue
            r.raise_for_status()
            return r.json()
        except RequestException as e:
            wait = BACKOFF_BASE**(attempt-1)
            print(f"  Err {e!r}, retry {attempt}/{RETRIES} in {wait}s…")
            time.sleep(wait)
    print(f"⚠ give up after {RETRIES} retries: {url}")
    return None


def main():
    if not API_KEY:
        print("⚠ SOLANATRACKER_API_KEY not set; exiting.")
        return

    mints = load_mints(TOKENS_CSV)
    now_ts = int(time.time())
    start_ts = now_ts - WINDOW_DAYS*24*3600

    headers = {"x-api-key": API_KEY}
    params = {"type": INTERVAL, "time_from": start_ts, "time_to": now_ts}

    rows = []
    for mint in mints:
        print(f"⏳ Fetching holders for {mint}…")
        data = fetch_json(HOLDERS_EP.format(mint=mint), params, headers)
        if data and "holders" in data:
            for entry in data["holders"]:
                # convert to ISO8601 UTC string
                ts = pd.to_datetime(entry["time"], unit='s', utc=True)
                iso_ts = ts.strftime('%Y-%m-%dT%H:%M:%SZ')
                rows.append({
                    "timestamp":    iso_ts,
                    "token_mint":   mint,
                    "holder_count": entry.get("holders")
                })
        else:
            print(f"  ℹ️  no data for {mint}")
        time.sleep(DELAY_REQ)

    if not rows:
        print("⚠ No holder data fetched—nothing to write.")
        return

    df = pd.DataFrame(rows)
    df = (
        df.dropna(subset=["holder_count"])  \
          .drop_duplicates(subset=["timestamp","token_mint"])  \
          .sort_values(["timestamp","token_mint"]).
          reset_index(drop=True)
    )
    df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Wrote {len(df)} rows → {OUTPUT_FILE}")


if __name__ == "__main__":
    main()

# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_transfers.py"

import time
import requests
import pandas as pd

# ── Configuration ──────────────────────────────────────────────────────────────
API_KEY = "aa77f73b-2247-43df-be03-b5e0dd8fb223"
MINT    = "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"

# 6-month window
NOW   = int(time.time())
START = NOW - 180 * 24 * 3600  # 180 days ago

# Aggregation frequency
BIN_FREQ = '12h'
# Helius only supports up to 100 items per page on this endpoint
LIMIT = 100

def fetch_token_transfers(mint: str,
                          start_ts: int,
                          end_ts: int,
                          api_key: str,
                          limit: int = LIMIT) -> list:
    """
    Retrieve all SPL TRANSFER events for `mint`, using Helius Enhanced Transactions API,
    paginating with `before=<last_signature>` and client-side timestamp filtering.
    """
    url = f"https://api.helius.xyz/v0/addresses/{mint}/transactions"
    before = None
    transfers = []
    page = 0

    while True:
        page += 1
        params = {
            "api-key": api_key,
            "type": "TRANSFER",   # filter to SPL/native transfers :contentReference[oaicite:0]{index=0}
            "limit": limit
        }
        if before:
            params["before"] = before  # page older than this signature :contentReference[oaicite:1]{index=1}

        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        batch = resp.json()
        if not batch:
            print(f"Page {page}: no more events, stopping.")
            break

        # Gather timestamps to know how far back we went
        ts_list = [evt.get("timestamp", 0) for evt in batch]
        oldest = min(ts_list)
        newest = max(ts_list)
        print(f"Page {page}: fetched {len(batch)} events  |  newest ts = {newest}  |  oldest ts = {oldest}")

        # Keep only those within our time window
        for evt in batch:
            ts = evt.get("timestamp", 0)
            if start_ts <= ts <= end_ts:
                transfers.append(evt)

        # If the oldest event is already before our window, we’re done
        if oldest < start_ts:
            print(f"Reached events older than {start_ts}; stopping pagination.")
            break

        # Otherwise, page again from the last signature
        before = batch[-1]["signature"]

    return transfers

def aggregate_transfers(transfers: list, bin_freq: str) -> pd.DataFrame:
    """
    Aggregate native and token transfer amounts into time bins.
    """
    records = []
    for evt in transfers:
        dt = pd.to_datetime(evt["timestamp"], unit='s')
        native_amt = sum(nt.get("amount", 0) for nt in evt.get("nativeTransfers", []))
        token_amt  = sum(tt.get("tokenAmount", 0) for tt in evt.get("tokenTransfers", []))
        records.append((dt, native_amt, token_amt))

    df = pd.DataFrame(records, columns=["datetime", "native_amount", "token_amount"])
    df.set_index("datetime", inplace=True)
    return df.resample(bin_freq).sum().reset_index()

def main():
    start_iso = pd.to_datetime(START, unit='s').isoformat()
    end_iso   = pd.to_datetime(NOW,   unit='s').isoformat()
    print(f"Fetching TRANSFER events for mint {MINT} between {start_iso} and {end_iso}…")

    transfers = fetch_token_transfers(MINT, START, NOW, API_KEY)
    print(f"\nTotal transfers in window: {len(transfers)}")

    df = aggregate_transfers(transfers, BIN_FREQ)
    df.to_csv('transfers.csv', index=False)
    print("Saved aggregated transfers to transfers.csv")
    print(df.head(), "…", df.tail(), sep="\n")

if __name__ == "__main__":
    main()
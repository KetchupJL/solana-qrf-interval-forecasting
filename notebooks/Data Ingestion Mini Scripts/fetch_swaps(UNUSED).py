# .\venv\Scripts\Activate.ps1
# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_swaps.py"

import time, requests, pandas as pd

API_KEY = "aa77f73b-2247-43df-be03-b5e0dd8fb223"
MINT    = "4k3Dyjzvzp8eMZWUXbBCjEvwSkkk59S5iCNLY3QrkX6R"

# 6 months window
NOW   = int(time.time())
START = NOW - 180 * 24 * 3600  # 180 days
# 12-hour bins frequency
BIN_FREQ = '12h'

# ── Helper ─────────────────────────────────────────────────────────────────────
def fetch_swap_events(mint, start_iso, end_iso):
    """
    Fetches all AMM swap events for `mint` over [start_iso, end_iso].
    Returns JSON list of events, or empty list if none/404.
    """
    url = f"https://api.helius.xyz/v0/token/swap-events/{mint}"
    params = {"api-key": API_KEY, "startTime": start_iso, "endTime": end_iso}
    try:
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 404:
            print(f"No swap events for {mint} (404). Returning empty list.")
            return []
        resp.raise_for_status()
        return resp.json()
    except requests.exceptions.HTTPError as e:
        print(f"HTTP error fetching swaps: {e}. Returning empty list.")
        return []
    except requests.exceptions.RequestException as e:
        print(f"Network error fetching swaps: {e}. Returning empty list.")
        return []

# ── Aggregation ────────────────────────────────────────────────────────────────
def aggregate_swaps(swaps, bins):
    df = pd.DataFrame(swaps)
    if df.empty:
        return pd.DataFrame(columns=["timestamp","avg_swap_size","swap_count"])
    df['ts'] = pd.to_datetime(df['timestamp'], unit='s')
    df.set_index('ts', inplace=True)
    out = []
    for lo, hi in zip(bins[:-1], bins[1:]):
        slice = df[lo:hi]
        avg_size = slice['amount'].astype(float).mean() if not slice.empty else 0
        out.append({
            "timestamp": lo,
            "avg_swap_size": avg_size,
            "swap_count": len(slice)
        })
    return pd.DataFrame(out)

# ── Main ───────────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    # Build 12-hour bins
    idx = pd.date_range(
        start=pd.to_datetime(START, unit='s'),
        end=pd.to_datetime(NOW,   unit='s'),
        freq=BIN_FREQ
    )
    start_iso = idx[0].isoformat()
    end_iso   = idx[-1].isoformat()

    print(f"Fetching swap events for mint {MINT} between {start_iso} and {end_iso}…")
    swaps = fetch_swap_events(MINT, start_iso, end_iso)
    df_s = aggregate_swaps(swaps, idx)
    df_s.to_csv('swaps.csv', index=False)
    print("Swaps aggregated (12h bins):", df_s.shape)
    print(df_s.head(), "…", df_s.tail(), sep="\n")

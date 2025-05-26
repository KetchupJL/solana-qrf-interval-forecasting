# same imports & config as fetch_transfers.py

BASE = "https://api.helius.dev/v0/token/swap-events"

def fetch_swaps(mint, start_iso, end_iso):
    resp = requests.get(
      f"{BASE}/{mint}",
      params={"api-key":API_KEY, "startTime":start_iso, "endTime":end_iso},
      timeout=30
    )
    resp.raise_for_status()
    return resp.json()  # list of {amount,price,...}

def aggregate(swaps, bins):
    df = pd.DataFrame(swaps)
    df['ts'] = pd.to_datetime(df['timestamp'], unit='s')
    df.set_index('ts', inplace=True)
    out = []
    for lo,hi in zip(bins[:-1], bins[1:]):
        slice = df[lo:hi]
        out.append({
          "timestamp": lo,
          "avg_swap_size": slice['amount'].astype(float).mean() or 0,
          "swap_count": len(slice)
        })
    return pd.DataFrame(out)

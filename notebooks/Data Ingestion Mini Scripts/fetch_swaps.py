import time, requests, pandas as pd

API_KEY = "aa77f73b-2247-43df-be03-b5e0dd8fb223"
MINT    = "<YOUR_TOKEN_MINT>"
BIN_SEC = 12 * 3600

def fetch_all(mint, start, end):
    url    = f"https://api.helius.dev/v0/token/swap-events/{mint}"
    params = {
      "api-key": API_KEY,
      "startTime":pd.to_datetime(start, unit='s').isoformat(),
      "endTime":pd.to_datetime(end, unit='s').isoformat()
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def aggregate(swaps, start, end):
    df = pd.DataFrame(swaps)
    df['ts'] = pd.to_datetime(df['timestamp'], unit='s')
    df.set_index('ts', inplace=True)
    bins = pd.date_range(start=pd.to_datetime(start, unit='s'),
                         end=pd.to_datetime(end, unit='s'),
                         freq='12h')
    out=[]
    for lo,hi in zip(bins[:-1], bins[1:]):
        slice = df[lo:hi]
        out.append({
          "timestamp": lo,
          "avg_swap_size": slice['amount'].astype(float).mean() if len(slice) else 0,
          "swap_count": len(slice)
        })
    return pd.DataFrame(out)

if __name__=="__main__":
    import pandas as pd, time
    now   = int(time.time())
    start = now - 180*24*3600
    swaps = fetch_all(MINT, start, now)
    df    = aggregate(swaps, start, now)
    df.to_csv("swaps.csv", index=False)
    print(df.head(), "\n…", df.tail())

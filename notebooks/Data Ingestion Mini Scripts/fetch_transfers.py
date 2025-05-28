import time, requests, pandas as pd

API_KEY = "aa77f73b-2247-43df-be03-b5e0dd8fb223"
MINT    = "<YOUR_TOKEN_MINT>"
BIN_SEC = 12 * 3600  # 12 h

def fetch_all(mint, start, end):
    url = f"https://api.helius.dev/v0/token/transfers/{mint}"
    params = {
      "api-key": API_KEY,
      "startTime":pd.to_datetime(start, unit='s').isoformat(),
      "endTime":pd.to_datetime(end, unit='s').isoformat()
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

def aggregate(transfers, start, end):
    df = pd.DataFrame(transfers)
    df['ts'] = pd.to_datetime(df['timestamp'], unit='s')
    df.set_index('ts', inplace=True)
    bins = pd.date_range(start=pd.to_datetime(start, unit='s'),
                         end=pd.to_datetime(end, unit='s'),
                         freq='12h')
    out = []
    for lo,hi in zip(bins[:-1], bins[1:]):
        slice = df[lo:hi]
        out.append({
          "timestamp": lo,
          "unique_wallets": slice[['from','to']].stack().nunique(),
          "tx_count": len(slice)
        })
    return pd.DataFrame(out)

if __name__=="__main__":
    import pandas as pd, time
    now   = int(time.time())
    start = now - 180*24*3600
    transfers = fetch_all(MINT, start, now)
    df = aggregate(transfers, start, now)
    df.to_csv("transfers.csv", index=False)
    print(df.head(), "\n…", df.tail())

import os, time, pandas as pd, requests

MINT    = "<your-mint>"
BASE    = "https://api.helius.dev/v0/token/transfers"
API_KEY     = os.getenv('HELIUS_API_KEY')

def fetch_transfers(mint, start_iso, end_iso):
    url    = f"{BASE}/{mint}"
    params = {"api-key": API_KEY, "startTime":start_iso, "endTime":end_iso}
    resp   = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()  # list of {from,to,timestamp,...}

def aggregate(transfers, bin_edges):
    df = pd.DataFrame(transfers)
    df['ts'] = pd.to_datetime(df['timestamp'], unit='s')
    df.set_index('ts', inplace=True)
    out = []
    for lo, hi in zip(bin_edges[:-1], bin_edges[1:]):
        slice = df[lo:hi]
        out.append({
          "timestamp": lo,
          "unique_wallets": slice[['from','to']].stack().nunique(),
          "tx_count": len(slice)
        })
    return pd.DataFrame(out)

if __name__=="__main__":
    # build 12h bins over 6mo
    now = pd.Timestamp.utcnow().floor('12h')
    bins = pd.date_range(now - pd.Timedelta(days=180), now, freq='12h')
    transfers = fetch_transfers(MINT, bins[0].isoformat(), bins[-1].isoformat())
    agg = aggregate(transfers, bins)
    agg.to_csv("transfers.csv", index=False)
    print(agg.head())

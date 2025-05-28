# .\venv\Scripts\Activate.ps1
# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_sol_price.py"

# fetch_sol_price.py (fixed)
import time, requests, pandas as pd
import os

os.makedirs("data", exist_ok=True)

COINGECKO  = "https://api.coingecko.com/api/v3"
CHUNK_SEC   = 30 * 24 * 3600   # 30-day slices
CHUNK_DELAY = 10               # wait 10 s between chunks
RETRIES     = 5

def safe_get(url, params):
    for i in range(RETRIES):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429:
            wait = 2**i
            print(f"429 from CG, sleeping {wait}s…")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("CoinGecko failed after retries")

def fetch_sol(start, end):
    parts = []
    for s in range(start, end, CHUNK_SEC):
        t   = min(s+CHUNK_SEC, end)
        url = f"{COINGECKO}/coins/solana/market_chart/range"
        data = safe_get(url, {'vs_currency':'usd','from':s,'to':t}).get('prices',[])
        if data:
            df = pd.DataFrame(data, columns=['ms','price_usd'])
            df['timestamp'] = pd.to_datetime(df['ms'], unit='ms')
            parts.append(df[['timestamp','price_usd']])
        time.sleep(CHUNK_DELAY)
    all_df = pd.concat(parts, ignore_index=True).drop_duplicates('timestamp')
    return all_df.set_index('timestamp')

if __name__=="__main__":
    now   = int(time.time())
    start = now - 180*24*3600
    df    = fetch_sol(start, now)
    ohlc  = df['price_usd'].resample('12h').ohlc()
    vol   = df['price_usd'].resample('12h').sum()
    ohlc['volume']=vol
    ohlc.to_csv("data/sol_price.csv")
    print(ohlc.head(), "\n…", ohlc.tail())
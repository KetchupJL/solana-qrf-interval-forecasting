import time, requests, pandas as pd

COINGECKO = "https://api.coingecko.com/api/v3"
CHUNK_SEC  = 7 * 24 * 3600  # 7-day slices
DELAY      = 5              # seconds between slices

def safe_get(url, params):
    for i in range(5):
        r = requests.get(url, params=params, timeout=30)
        if r.status_code == 429:
            wait = 2**i
            print(f"429, sleeping {wait}s…")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    raise RuntimeError("CoinGecko failed after 5 retries")

def fetch_sol(start, end):
    dfs = []
    for s in range(start, end, CHUNK_SEC):
        t = min(s+CHUNK_SEC, end)
        url = f"{COINGECKO}/coins/solana/market_chart/range"
        data = safe_get(url, {'vs_currency':'usd','from':s,'to':t}).get('prices',[])
        df = pd.DataFrame(data, columns=['ms','price_usd'])
        df['timestamp'] = pd.to_datetime(df['ms'], unit='ms')
        dfs.append(df[['timestamp','price_usd']])
        time.sleep(DELAY)
    all_df = pd.concat(dfs).drop_duplicates('timestamp').set_index('timestamp')
    return all_df

if __name__=="__main__":
    now   = int(time.time())
    start = now - 180*24*3600
    df = fetch_sol(start, now)
    df_12h = df['price_usd'].resample('12h').ohlc()
    df_12h['volume']=df['price_usd'].resample('12h').sum()
    df_12h.to_csv("sol_price.csv")
    print(df_12h.head(), "\n…", df_12h.tail())

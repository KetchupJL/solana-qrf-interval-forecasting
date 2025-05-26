import os, time, pandas as pd, requests

API_KEY = os.getenv("LUNARCRUSH_KEY")
SYMBOL  = "FART"  # or map from mint→symbol
BASE    = "https://api.lunarcrush.com/v2"

def fetch_social(symbol, start_unix, end_unix):
    url = f"{BASE}?data=assets&key={API_KEY}"
    params = {
      "symbol_list": symbol,
      "interval": "12h",
      "start": start_unix,
      "end": end_unix
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()['data']

if __name__=="__main__":
    now = int(time.time())
    start = now - 180*24*3600
    data = fetch_social(SYMBOL, start, now)
    df = pd.DataFrame(data)[[
      'time','social_volume','sentiment_score'
    ]]
    df['timestamp'] = pd.to_datetime(df['time'], unit='s')
    df.to_csv("social.csv", index=False)
    print(df.head())

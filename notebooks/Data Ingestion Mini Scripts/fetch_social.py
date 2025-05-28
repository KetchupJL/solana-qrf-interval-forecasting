import time, requests, pandas as pd

API_KEY = "<YOUR_LUNARCRUSH_KEY>"
SYMBOL  = "<TOKEN_SYMBOL_OR_COMMA_LIST>"
INTERVAL= "12h"
NOW     = int(time.time())
START   = NOW - 180*24*3600

def fetch_social(sym, start, end):
    url = "https://api.lunarcrush.com/v2"
    params = {
      "data":"assets",
      "key":API_KEY,
      "symbol_list":sym,
      "start":start,
      "end":end,
      "interval":INTERVAL
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json().get('data',[])

if __name__=="__main__":
    data = fetch_social(SYMBOL, START, NOW)
    df   = pd.DataFrame(data)
    df['timestamp'] = pd.to_datetime(df['time'],unit='s')
    df = df[['timestamp','social_volume','sentiment_score']]
    df.to_csv("social.csv", index=False)
    print(df.head(), "\n…", df.tail())

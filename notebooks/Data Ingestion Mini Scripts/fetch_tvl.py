import requests, pandas as pd

url  = "https://api.llama.fi/charts/solana"
resp = requests.get(url, timeout=30)
resp.raise_for_status()
data = resp.json()  # list of [unix_ms, tvl_usd]

df = pd.DataFrame(data, columns=['ms','tvl_usd'])
df['timestamp'] = pd.to_datetime(df['ms'], unit='ms')
df.set_index('timestamp', inplace=True)
# Resample to 12h and compute % change
tv = df['tvl_usd'].resample('12h').last()
tvc = tv.pct_change().fillna(0)
out = pd.DataFrame({
  'timestamp': tv.index,
  'tvl_usd': tv.values,
  'tvl_change_12h': tvc.values
})
out.to_csv("tvl.csv", index=False)
print(out.head())

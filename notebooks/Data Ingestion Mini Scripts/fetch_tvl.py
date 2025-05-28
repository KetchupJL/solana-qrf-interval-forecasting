import requests, pandas as pd

r = requests.get("https://api.llama.fi/charts/solana", timeout=30)
r.raise_for_status()
data = r.json()  # [[ms,tvl],…]
df   = pd.DataFrame(data, columns=['ms','tvl_usd'])
df['timestamp'] = pd.to_datetime(df['ms'],unit='ms')
df.set_index('timestamp', inplace=True)
tv = df['tvl_usd'].resample('12h').last()
tvc= tv.pct_change().fillna(0)
out= pd.DataFrame({
    'timestamp':tv.index,
    'tvl_usd':tv.values,
    'tvl_change_12h':tvc.values
})
out.to_csv("tvl.csv", index=False)
print(out.head(), "\n…", out.tail())

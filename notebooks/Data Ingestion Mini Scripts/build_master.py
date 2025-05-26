import pandas as pd
dfs = [
  pd.read_csv("price.csv"),
  pd.read_csv("holders.csv"),
  pd.read_csv("transfers.csv"),
  pd.read_csv("swaps.csv"),
  pd.read_csv("social.csv"),
  pd.read_csv("tvl.csv"),
]
master = dfs[0]
for df in dfs[1:]:
    master = master.merge(df, on='timestamp', how='left')
master.to_parquet("data/solana_master_12h.parquet", index=False)

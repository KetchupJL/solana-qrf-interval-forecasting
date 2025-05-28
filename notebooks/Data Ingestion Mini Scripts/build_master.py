# src/ingest/build_master.py
import pandas as pd

def load(name):
    return pd.read_csv(f"data/{name}.csv", parse_dates=["timestamp"])

def main():
    price      = load("token_price")
    holders    = load("holders")
    sol        = load("sol_price")
    tvl        = load("tvl")
    btc_eth    = load("btc_eth_price")
    # optionally defi_tvl = load("defi_tvl")

    master = price
    for df in (holders, sol, tvl, btc_eth):
        master = master.merge(df, on="timestamp", how="left")

    # final cleaning…
    master.to_parquet("data/solana_master_12h.parquet", index=False)
    print("Master built:", master.shape)

if __name__=="__main__":
    main()

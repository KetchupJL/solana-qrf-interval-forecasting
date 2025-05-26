# python scripts/sol_vs_usd.py


def fetch_sol_price_usd(mint, start, end):
    dfs = []
    for s in range(start, end, CHUNK_SEC):
        t = min(s + CHUNK_SEC, end)
        url = f"{COINGECKO_API}/coins/solana/market_chart/range"
        params = {'vs_currency':'usd','from':s,'to':t}
        data = safe_get_coingecko(url, params).get('prices', [])
        if data:
            df = pd.DataFrame(data, columns=['ms','price_usd'])
            df['timestamp'] = pd.to_datetime(df['ms'], unit='ms')
            dfs.append(df[['timestamp','price_usd']])
        # throttle to avoid rate-limit
        time.sleep(CHUNK_DELAY)
    if not dfs:
        return pd.DataFrame(columns=['timestamp','price_usd'])
    all_df = pd.concat(dfs, ignore_index=True).drop_duplicates(subset=['timestamp'])
    return all_df.set_index('timestamp')
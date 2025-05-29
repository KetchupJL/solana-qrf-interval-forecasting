# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_social_sentiment.py"

#!/usr/bin/env python3
"""
fetch_social_sentiment.py

Backfill N days of 12 h–binned social attention metrics for each SPL token:
  • Twitter mentions ($TICKER) via snscrape
  • Reddit comments (r/TICKER) via Pushshift
  • Google Trends interest
  • LunarCrush social_volume

Usage:
  python fetch_social_sentiment.py \
    --tokens tokens.csv \
    --out-dir data/ \
    --window-days 180 \
    --bin-freq 12H \
    --workers 4
"""
import os, time, argparse, logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
from pytrends.request import TrendReq
from dotenv import load_dotenv

# ── Setup & Config ─────────────────────────────────────────────────────────
load_dotenv()   # loads LUNARCRUSH_KEY, etc.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("social_sentiment")

# LunarCrush key
LUNAR_KEY = os.getenv("LUNARCRUSH_KEY")
if not LUNAR_KEY:
    logger.warning("No LUNARCRUSH_KEY: social_volume will be zero")

# HTTP session w/ retries
def make_session():
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=0.3,
                    status_forcelist=[429,500,502,503,504])
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

session = make_session()

# Google Trends init
try:
    pytrends = TrendReq(hl="en-US", tz=0,
                        retries=3, backoff_factor=0.5,
                        timeout=(10,30))
except Exception as e:
    logger.error(f"Google Trends init failed: {e}; will emit zeros")
    pytrends = None

# ── Argparse ────────────────────────────────────────────────────────────────
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--tokens",      default="tokens.csv",
                   help="Headerless CSV: mint,symbol")
    p.add_argument("--out-dir",     default="data/",
                   help="Where to save outputs")
    p.add_argument("--window-days", type=int, default=180,
                   help="Backfill window in days")
    p.add_argument("--bin-freq",    default="12H",
                   help="Resample freq (e.g. 12H)")
    p.add_argument("--workers",     type=int, default=1,
                   help="Parallel token fetches")
    return p.parse_args()

# ── Data Prep ───────────────────────────────────────────────────────────────
def make_bins(days: int, freq: str) -> pd.DatetimeIndex:
    now   = datetime.now(timezone.utc)
    start = now - timedelta(days=days)
    freq_norm = freq.replace("H","h")
    return pd.date_range(start=start, end=now, freq=freq_norm)

def load_tokens(path: Path):
    df = pd.read_csv(path, header=None, usecols=[0,1],
                     names=["mint","symbol"])
    df = df.dropna(subset=["mint","symbol"])
    df["symbol"] = df["symbol"].astype(str).str.strip().str.upper()
    return df.to_dict("records")

# ── Fetchers ────────────────────────────────────────────────────────────────
import snscrape.modules.twitter as sntwitter

def fetch_twitter(symbol: str, bins: pd.DatetimeIndex) -> pd.Series:
    """
    Scrape tweet‐counts for each 12 h bin using snscrape.
    """
    counts = []
    for t0, t1 in zip(bins[:-1], bins[1:]):
        query = f"${symbol} since:{t0.date()} until:{t1.date()}"
        try:
            cnt = sum(1 for _ in sntwitter.TwitterSearchScraper(query).get_items())
        except Exception as e:
            logger.error(f"Twitter scrape @{t0} {symbol}: {e}")
            cnt = 0
        counts.append(cnt)
        time.sleep(0.2)
    return pd.Series(counts, index=bins[:-1], name="twitter_count")

def fetch_reddit(symbol: str, bins: pd.DatetimeIndex) -> pd.Series:
    counts = []
    for t0, t1 in zip(bins[:-1], bins[1:]):
        try:
            r = session.get(
                "https://api.pushshift.io/reddit/search/comment",
                params={
                  "subreddit": symbol.lower(),
                  "after": int(t0.timestamp()),
                  "before": int(t1.timestamp()),
                  "size": 0
                }, timeout=10
            ).json()
            cnt = r.get("metadata",{}).get("total_results",0)
        except Exception as e:
            logger.error(f"Reddit @{t0} {symbol}: {e}")
            cnt = 0
        counts.append(cnt)
        time.sleep(0.2)
    return pd.Series(counts, index=bins[:-1], name="reddit_count")

def fetch_trends(symbol: str, bins: pd.DatetimeIndex) -> pd.Series:
    if not pytrends:
        return pd.Series(0, index=bins[:-1], name="google_trends")
    try:
        tf = f"{bins[0].strftime('%Y-%m-%dT%H')} {bins[-1].strftime('%Y-%m-%dT%H')}"
        pytrends.build_payload([symbol], timeframe=tf)
        df = pytrends.interest_over_time()
        s = df[symbol].resample(bins.freq).mean()
        s = s.reindex(bins[:-1], method="ffill").fillna(0)
        s.name = "google_trends"
        return s
    except Exception as e:
        logger.error(f"Google Trends @{symbol}: {e}")
        return pd.Series(0, index=bins[:-1], name="google_trends")

def fetch_lunarcrush(bins: pd.DatetimeIndex, symbols: list) -> pd.Series:
    if not os.getenv("LUNARCRUSH_KEY"):
        return pd.Series(0, index=bins[:-1], name="social_volume")
    params = {
      "data":"assets",
      "key": os.getenv("LUNARCRUSH_KEY"),
      "symbol_list": ",".join(symbols),
      "start": int(bins[0].timestamp()),
      "end":   int(bins[-1].timestamp()),
      "interval": bins.freqstr.lower()
    }
    try:
        r = session.get("https://api.lunarcrush.com/v2",
                        params=params, timeout=20)
        r.raise_for_status()
        df = pd.DataFrame(r.json().get("data",[]))
        df["timestamp"] = pd.to_datetime(df["time"], unit="s")
        series = df.set_index("timestamp")["social_volume"]
        return series.reindex(bins[:-1], method="ffill").fillna(0)
    except Exception as e:
        logger.error(f"LunarCrush fetch: {e}")
        return pd.Series(0, index=bins[:-1], name="social_volume")

# ── Worker & Main ──────────────────────────────────────────────────────────
def process_token(tok, bins, all_symbols):
    mint, sym = tok["mint"], tok["symbol"]
    logger.info(f"→ Processing {sym} ({mint})")
    tw = fetch_twitter(sym, bins)
    rd = fetch_reddit(sym, bins)
    gt = fetch_trends(sym, bins)
    lc = fetch_lunarcrush(bins, all_symbols)
    df = pd.concat([tw, rd, gt, lc], axis=1)
    df["timestamp"]   = df.index
    df["token_mint"]  = mint
    return df.reset_index(drop=True)

def main():
    args    = parse_args()
    outdir  = Path(args.out_dir); outdir.mkdir(exist_ok=True, parents=True)

    bins   = make_bins(args.window_days, args.bin_freq)
    toks   = load_tokens(Path(args.tokens))
    syms   = [t["symbol"] for t in toks]

    dfs = []
    if args.workers>1:
        with ThreadPoolExecutor(max_workers=args.workers) as ex:
            futures = [ex.submit(process_token, t, bins, syms) for t in toks]
            for f in as_completed(futures):
                dfs.append(f.result())
    else:
        for t in toks:
            dfs.append(process_token(t, bins, syms))

    result = pd.concat(dfs, ignore_index=True)
    for c in ["twitter_count","reddit_count","google_trends","social_volume"]:
        result[c] = pd.to_numeric(result[c], errors="coerce").fillna(0)

    result.to_csv(outdir/"social_sentiment.csv", index=False)
    result.to_parquet(outdir/"social_sentiment.parquet",
                      index=False, compression="snappy")
    logger.info(f"✅ Wrote {len(result)} rows to {outdir}")

if __name__=="__main__":
    main()

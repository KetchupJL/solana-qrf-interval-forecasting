# .\venv\Scripts\Activate.ps1
# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\fetch_all_twitter_sentiment.py" \--tokens-file tokens.csv \--start-date 2024-11-30 \--end-date   2025-05-29 \--max-per-day 250 \--workers 4 \--output-file all_twitter_sentiment.csv


#!/usr/bin/env python3
"""
fetch_all_twitter_sentiment.py

Fetch six months (or arbitrary date range) of daily Twitter sentiment
for **all tickers** listed in tokens.csv.  Uses SNScrape + NLTK/VADER,
but now optimized for speed by:
  1. Limiting to 250 tweets/day (default).
  2. Running each ticker’s date‐loop in parallel (configurable via --workers).

Requirements:
    pip install snscrape pandas nltk

Before running the first time:
    python3 -c "import nltk; nltk.download('vader_lexicon')"

Usage example:
    python fetch_all_twitter_sentiment.py \
        --tokens-file tokens.csv \
        --start-date 2024-11-30 \
        --end-date   2025-05-29 \
        --max-per-day 250 \
        --workers 4 \
        --output-file all_twitter_sentiment.csv

Outputs a single CSV with columns:
    ticker, date, total_tweets, avg_compound, positive_count, neutral_count, negative_count
"""

import argparse
import datetime
import pandas as pd
import snscrape.modules.twitter as sntwitter
import nltk
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from nltk.sentiment.vader import SentimentIntensityAnalyzer

# Ensure VADER lexicon is downloaded
try:
    _ = SentimentIntensityAnalyzer()
except LookupError:
    nltk.download('vader_lexicon')
    _ = SentimentIntensityAnalyzer()


def parse_args():
    p = argparse.ArgumentParser(
        description="Fetch daily Twitter sentiment for every ticker in tokens.csv (parallelized)."
    )
    p.add_argument(
        "--tokens-file", type=str, default="tokens.csv",
        help="Path to CSV containing a column 'ticker' listing all tickers to scrape (default: tokens.csv)."
    )
    p.add_argument(
        "--start-date", type=str, required=True,
        help="Start date (inclusive) in YYYY-MM-DD format."
    )
    p.add_argument(
        "--end-date", type=str, required=True,
        help="End date (inclusive) in YYYY-MM-DD format."
    )
    p.add_argument(
        "--max-per-day", type=int, default=250,
        help="Maximum number of tweets to scrape per ticker per day (default: 250)."
    )
    p.add_argument(
        "--workers", type=int, default=4,
        help="Number of tickers to process in parallel (default: 4)."
    )
    p.add_argument(
        "--output-file", type=str, default="all_twitter_sentiment.csv",
        help="Output CSV filename for the combined dataset (default: all_twitter_sentiment.csv)."
    )
    return p.parse_args()


def daterange(start_date: datetime.date, end_date: datetime.date):
    """
    Yield each date from start_date up to and including end_date.
    """
    for n in range(int((end_date - start_date).days) + 1):
        yield start_date + datetime.timedelta(n)


def scrape_one_day_for_ticker(ticker: str, day: datetime.date, max_tweets: int):
    """
    Scrape up to max_tweets tweets mentioning `ticker` on the given `day`,
    compute VADER sentiment for each, and return a dict of daily aggregates.
    """
    sid = SentimentIntensityAnalyzer()
    since_str = day.strftime("%Y-%m-%d")
    until_str = (day + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

    # Build the SNScrape query for that calendar day
    query = f'"{ticker}" since:{since_str} until:{until_str}'
    scraper = sntwitter.TwitterSearchScraper(query)

    total = 0
    sum_compound = 0.0
    positive_count = 0
    neutral_count = 0
    negative_count = 0

    # Loop up to `max_tweets` items
    for i, tweet in enumerate(scraper.get_items()):
        if i >= max_tweets:
            break
        text = tweet.content
        vs = sid.polarity_scores(text)
        comp = vs["compound"]
        sum_compound += comp
        if comp >= 0.05:
            positive_count += 1
        elif comp <= -0.05:
            negative_count += 1
        else:
            neutral_count += 1
        total += 1

    if total > 0:
        avg_compound = sum_compound / total
    else:
        avg_compound = 0.0

    return {
        "ticker": ticker,
        "date": day.strftime("%Y-%m-%d"),
        "total_tweets": total,
        "avg_compound": avg_compound,
        "positive_count": positive_count,
        "neutral_count": neutral_count,
        "negative_count": negative_count
    }


def process_ticker(ticker: str, start_date: datetime.date, end_date: datetime.date, max_per_day: int):
    """
    For one ticker, loop over each day between start_date and end_date (inclusive),
    scrape sentiment and return a list of daily‐aggregate dicts.
    """
    results = []
    for single_day in daterange(start_date, end_date):
        # Optional: print a short progress log (comment out if too verbose)
        print(f"  • [{ticker}] {single_day} ... ", end="", flush=True)

        day_stats = scrape_one_day_for_ticker(ticker, single_day, max_per_day)

        print(f"{day_stats['total_tweets']} tweets, avg_compound={day_stats['avg_compound']:.3f}")
        results.append(day_stats)

        # (Optional) tiny sleep to avoid any hidden rate‐limit thrashing:
        # time.sleep(0.1)

    return results


def main():
    args = parse_args()

    # Read and validate tokens.csv
    try:
        tokens_df = pd.read_csv(args.tokens_file, dtype=str)
    except FileNotFoundError:
        print(f"✖ Could not find tokens file: {args.tokens_file}")
        sys.exit(1)

    if "ticker" not in tokens_df.columns:
        print("✖ tokens.csv must have a column named 'ticker'")
        sys.exit(1)

    # Extract and sanitize the list of tickers
    tickers = tokens_df["ticker"].dropna().astype(str).str.strip().tolist()
    if len(tickers) == 0:
        print("✖ Found zero tickers in tokens.csv.")
        sys.exit(1)

    # Parse start/end dates
    try:
        start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d").date()
        end_date   = datetime.datetime.strptime(args.end_date, "%Y-%m-%d").date()
    except ValueError as e:
        print(f"✖ Error parsing dates: {e}")
        sys.exit(1)
    if end_date < start_date:
        print("✖ end-date must be on or after start-date.")
        sys.exit(1)

    print(f"ℹ️  Running daily Twitter sentiment from {start_date} → {end_date}")
    print(f"ℹ️  {len(tickers)} tickers to process, up to {args.max_per_day} tweets/day each.")
    print(f"ℹ️  Parallelizing across {args.workers} workers.\n")

    # We'll collect all daily‐stat dicts in this list
    all_results = []

    # Use a ThreadPoolExecutor to process each ticker in parallel
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        # Create a future for each ticker
        future_to_ticker = {
            executor.submit(
                process_ticker,
                ticker,
                start_date,
                end_date,
                args.max_per_day
            ): ticker
            for ticker in tickers
        }

        # As each ticker’s thread completes, gather its results
        for future in as_completed(future_to_ticker):
            t = future_to_ticker[future]
            try:
                ticker_results = future.result()
            except Exception as exc:
                print(f"✖ [ERROR] Ticker {t} generated an exception: {exc}")
                continue

            # Append all daily dicts for this ticker
            all_results.extend(ticker_results)

    # Build a DataFrame from all_results
    master_df = pd.DataFrame(all_results)

    # Sort by ticker, then date
    master_df = master_df.sort_values(["ticker", "date"]).reset_index(drop=True)

    # Save to CSV
    master_df.to_csv(args.output_file, index=False)
    print(f"\n✅ Wrote combined results ({len(master_df)} rows) to {args.output_file}")


if __name__ == "__main__":
    main()

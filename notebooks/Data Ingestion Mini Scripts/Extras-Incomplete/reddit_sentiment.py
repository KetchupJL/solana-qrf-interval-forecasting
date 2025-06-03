# python "C:\Users\james\OneDrive\Documents\GitHub\solana-qrf-interval-forecasting\notebooks\Data Ingestion Mini Scripts\reddit_sentiment.py"
# 
#
#!/usr/bin/env python3
"""


Fetch the last 6 months of Reddit comments mentioning specified token keywords
using the Pushshift API, run Vader sentiment analysis on each comment, bucket
results into 12‐hour UTC intervals, and save to CSV.

Dependencies (Python 3.13 compatible):
    pip install requests pandas nltk

"""

import time
import requests
import pandas as pd
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer

# ──────────────────────────────────────────────────────────────────────────────
# 1) Ensure Vader lexicon is downloaded
nltk.download("vader_lexicon", quiet=True)
sid = SentimentIntensityAnalyzer()

# ──────────────────────────────────────────────────────────────────────────────
# 2) CONFIGURATION

# List of token keywords (lowercase) to search for in comment bodies.
TOKENS = [
    "fartcoin",
    "ray",
    "mewtoken",
    # …add more token keywords here as needed…
]

# How many days back to fetch comments (180 days ≈ 6 months).
DAYS_BACK = 180

# Number of comments to fetch per Pushshift request (max 1000).
BATCH_SIZE = 1000

# Correct Pushshift endpoint for comment search.
PUSHSHIFT_URL = "https://api.pushshift.io/reddit/comment/search/"

# Minimum pause (in seconds) between Pushshift requests.
REQUEST_PAUSE = 1.0

# Where to save the output CSVs.
OUTPUT_DIR = "."

# If Pushshift returns a 403 or other error repeatedly, you can
# optionally give up after N retries per batch.
MAX_RETRIES_PER_BATCH = 5

# ──────────────────────────────────────────────────────────────────────────────
# 3) Helper functions

def fetch_all_comments_for_token(token: str, start_epoch: int, end_epoch: int) -> list[dict]:
    """
    Fetch all comments containing `token` between start_epoch and end_epoch
    using Pushshift. Pages backward in time by repeatedly setting `before`
    to (oldest_timestamp - 1). Returns a list of JSON dicts with at least
    "body" and "created_utc".
    """
    all_comments = []
    before = end_epoch

    while True:
        params = {
            "q": token,                     # search term (case-insensitive)
            "size": BATCH_SIZE,             # up to 1000 results per request
            "before": before,               # get items with created_utc < before
            "after": start_epoch,           # get items with created_utc > start_epoch
            "sort": "created_utc",          # sort by creation time
            "order": "desc"                 # newest first
        }

        success = False
        retries = 0
        while not success and retries < MAX_RETRIES_PER_BATCH:
            resp = requests.get(PUSHSHIFT_URL, params=params)
            if resp.status_code == 200:
                success = True
            else:
                retries += 1
                print(f"[{token}] Pushshift HTTP {resp.status_code} – retrying in 5s "
                      f"({retries}/{MAX_RETRIES_PER_BATCH})")
                time.sleep(5)

        if not success:
            print(f"[{token}] Failed to retrieve batch after {MAX_RETRIES_PER_BATCH} retries. "
                  "Continuing with what we have.")
            break

        data = resp.json().get("data", [])
        if not data:
            # No more comments in this time window
            break

        all_comments.extend(data)

        # The last item is the oldest comment in this batch (because sort=desc)
        oldest_ts = data[-1]["created_utc"]
        before = oldest_ts - 1  # move 'before' to one second older than the oldest comment

        # Politeness pause
        time.sleep(REQUEST_PAUSE)

    return all_comments


def floor_to_12h(ts: pd.Timestamp) -> pd.Timestamp:
    """
    Floors a UTC Timestamp to its 12h bucket:
      - Hours 00..11 → YYYY-MM-DD 00:00:00 UTC
      - Hours 12..23 → YYYY-MM-DD 12:00:00 UTC
    """
    date_mid = ts.normalize()  # midnight UTC of that date
    return date_mid if ts.hour < 12 else date_mid + pd.Timedelta(hours=12)


# ──────────────────────────────────────────────────────────────────────────────
# 4) Main execution

if __name__ == "__main__":
    now_epoch = int(time.time())
    start_epoch = now_epoch - DAYS_BACK * 24 * 3600

    for token in TOKENS:
        print(f"\n=== Fetching comments for '{token}' over the last {DAYS_BACK} days ===")
        comments = fetch_all_comments_for_token(token, start_epoch, now_epoch)
        total = len(comments)
        print(f"[{token}] Retrieved {total} comments")

        if total == 0:
            print(f"[{token}] No comments found, skipping sentiment analysis.\n")
            continue

        # Build DataFrame with only the fields we need
        df = pd.DataFrame(comments)
        df = df[["body", "created_utc"]].copy()

        # Convert created_utc (epoch seconds) to pandas.Timestamp (UTC)
        df["created_utc"] = pd.to_datetime(df["created_utc"], unit="s", utc=True)

        # Compute Vader compound sentiment for each comment body
        df["compound"] = df["body"].str.lower().map(lambda text: sid.polarity_scores(text)["compound"])

        # Floor each comment's timestamp to its 12h bucket
        df["bucket_start"] = df["created_utc"].map(floor_to_12h)

        # Aggregate: count of comments and average sentiment per bucket
        agg = (
            df.groupby("bucket_start")
              .agg(
                  mention_count=("body", "count"),
                  avg_compound_sentiment=("compound", "mean")
              )
              .reset_index()
        )

        # Add token column
        agg["token"] = token

        # Reorder columns to [token, bucket_start, mention_count, avg_compound_sentiment]
        agg = agg[["token", "bucket_start", "mention_count", "avg_compound_sentiment"]]

        # OPTIONAL: Fill missing 12h buckets with zeros
        # Uncomment this block if you want a continuous 12h index from start to now
        # full_range = pd.date_range(
        #     start=agg["bucket_start"].min(),
        #     end=agg["bucket_start"].max(),
        #     freq="12H",
        #     tz="UTC"
        # )
        # full_idx = pd.MultiIndex.from_product([[token], full_range],
        #                                       names=["token", "bucket_start"])
        # agg = agg.set_index(["token", "bucket_start"]).reindex(full_idx, fill_value=0).reset_index()

        # Save to CSV
        out_path = f"{OUTPUT_DIR}/reddit_sentiment_{token}.csv"
        agg.to_csv(out_path, index=False)
        print(f"[{token}] Saved sentiment CSV → {out_path}")

    print("\nAll tokens processed successfully.")

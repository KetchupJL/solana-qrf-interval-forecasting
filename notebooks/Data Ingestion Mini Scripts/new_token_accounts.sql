-- 12h new token accounts (first‐time holders)
WITH opened AS (
  SELECT
    mint AS token_mint_address,
    TIMESTAMP_TRUNC(block_timestamp, HOUR) AS ts_hour,
    account_address,
    balance
  FROM
    `bigquery-public-data.crypto_solana_mainnet_us.token_accounts`
  WHERE
    LOWER(mint) IN ( /* your 24 mints */ )
    AND balance > 0
    AND block_timestamp 
        BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY) 
            AND CURRENT_TIMESTAMP()
)
, first_open AS (
  -- identify the very first time each (mint, account) appears with balance>0
  SELECT
    token_mint_address,
    account_address,
    MIN(ts_hour) AS first_active_hour
  FROM opened
  GROUP BY token_mint_address, account_address
)
SELECT
  token_mint_address,

  CASE
    WHEN EXTRACT(HOUR FROM first_active_hour) < 12 
      THEN TIMESTAMP_TRUNC(first_active_hour, DAY) 
    ELSE TIMESTAMP_ADD(TIMESTAMP_TRUNC(first_active_hour, DAY), INTERVAL 12 HOUR)
  END AS bucket_start,

  COUNT(*) AS new_token_accounts

FROM first_open
GROUP BY token_mint_address, bucket_start
ORDER BY token_mint_address, bucket_start;

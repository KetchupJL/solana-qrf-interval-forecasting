-- 12h “true” holder count
SELECT
  mint AS token_mint_address,

  CASE
    WHEN EXTRACT(HOUR FROM block_timestamp) < 12
      THEN TIMESTAMP_TRUNC(block_timestamp, DAY) 
    ELSE TIMESTAMP_ADD(TIMESTAMP_TRUNC(block_timestamp, DAY), INTERVAL 12 HOUR)
  END AS bucket_start,

  COUNT(DISTINCT account_address) AS holder_count

FROM
  `bigquery-public-data.crypto_solana_mainnet_us.token_balances`

WHERE
  LOWER(mint) IN ( 
    /* your 24 lowercase mints here */ 
  )
  AND block_timestamp 
      BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY) 
          AND CURRENT_TIMESTAMP()

GROUP BY
  token_mint_address,
  bucket_start

ORDER BY
  token_mint_address,
  bucket_start;

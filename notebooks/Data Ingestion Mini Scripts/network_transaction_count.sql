-- 12h total network transactions
SELECT
  CASE
    WHEN EXTRACT(HOUR FROM block_timestamp) < 12
      THEN TIMESTAMP_TRUNC(block_timestamp, DAY)
    ELSE TIMESTAMP_ADD(TIMESTAMP_TRUNC(block_timestamp, DAY), INTERVAL 12 HOUR)
  END AS bucket_start,

  COUNT(*) AS tx_count

FROM
  `bigquery-public-data.crypto_solana_mainnet_us.Transactions`
WHERE
  block_timestamp 
    BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY) 
        AND CURRENT_TIMESTAMP()
GROUP BY bucket_start
ORDER BY bucket_start;

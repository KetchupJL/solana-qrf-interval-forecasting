-- 12 h SPL‐token transfer stats for the last 6 months
SELECT
  mint AS token_mint_address,

  CASE
    WHEN EXTRACT(HOUR FROM block_timestamp) < 12
      THEN TIMESTAMP_TRUNC(block_timestamp, DAY)
    ELSE TIMESTAMP_ADD(TIMESTAMP_TRUNC(block_timestamp, DAY), INTERVAL 12 HOUR)
  END AS bucket_start,

  COUNT(*)                    AS transfer_count,
  COUNT(DISTINCT source)      AS unique_senders,
  COUNT(DISTINCT destination) AS unique_receivers

FROM
  `bigquery-public-data.crypto_solana_mainnet_us.Token Transfers`

WHERE
  -- only transfers in the last 180 days (≈6 months)
  block_timestamp
    BETWEEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY)
        AND CURRENT_TIMESTAMP()

  AND LOWER(mint) IN (
    '9bb6nfecjbctnnlfko2fqvqbq8hhm13kcyycdqbgpump',
    '4k3dyjzvzp8emzwuxbbcjevwskkk59s5icnly3qrkx6r',
    'mew1gqwj3nexg2qgeriku7fafj79phvqvrequzscpp5',
    'ey59ph7z4bfu4hjyknymdwt5ggn76kaztawqihouxrnk',
    'c7heqqfnzdmbufqwchkl9fvdwsfsdrbnfwzddywycltz',
    'dku9kyksfbn5lbffxtnndpax35o4fv6vj9fkk7pzpump',
    'ekpqgsjtjmfqkz9kqansqyxrcf8fbopzlhyxdm65zcjm',
    '7gcihgdb8fe6knjn2mytkzzcrjqy3t9ghdc8uhymw2hr',
    '2qehjdldlbubgryvsxhc5d6udwaivnfzgan56p1tpump',
    'ed5nyywezpppiwimp8vym7sd7td3lat3q3grtwhzpjby',
    '63lfdmnb3mq8mw9mtz2to9bea2m71kzuugq5tijxcqj9',
    'ukhh6c7mmyiwcf1b9pnwe25tspkddt3h5pqsqzgz74j82',
    '5z3eqyqo9hices3r84rcdmu2n7anpdmxrhdk8pswmrrc',
    'a8c3xuqscfmylrte3vmtqraq8kgmasius9afnanwpump',
    'el5fuxj2j4ciqsmw85k5fg9dvuqjjuobhoqbi2kpump',
    'ftuew73k6veyhfbkfpdbzfwpxgqar2hipgdbutehpump',
    '8x5vqbha8d7nkd52unus5nnt3pwa8pld34ymskeso2wn',
    'hng5pyjmtqcmzxrv6s9zp1cdkk5bgduyfbxbvnapump',
    'czlsujwblfssjncfkh59rufqvafwcy5tzedwjsuypump',
    '6ogzhhzdrqr9pgv6hz2mnze7urzbmafybbwuyp1fhitx',
    '5mbk36sz7j19an8jfochhqs4of8g6bwujbecsxbsowdp',
    '5svg3t9cnqsm2kewzbrq6hasqh1ogfjqttlxyuibpump',
    '7xjiwldrjzxdydzipnjxzpr1idtmk55xixsfaa7jgnel'
  )

GROUP BY
  token_mint_address,
  bucket_start

ORDER BY
  token_mint_address,
  bucket_start;

-- 12h “cumulative holder count” per token, derived from token_transfers
WITH all_buckets AS (
  -- Generate every 12h bucket timestamp over the last 180 days
  SELECT
    bucket_start
  FROM (
    SELECT
      TIMESTAMP_TRUNC(
        TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 180 DAY),
        HOUR
      ) 
      + INTERVAL (b * 12) HOUR AS bucket_start
    FROM (
      -- Enough multiples of 12h to cover 180 days. 180 days = 360 12 h buckets.
      SELECT
        ROW_NUMBER() OVER() - 1 AS b
      FROM
        UNNEST(GENERATE_ARRAY(1, 400))  -- 400 > 360, just a safe upper bound
    )
  )
)

SELECT
  tt.token_mint_address,
  ab.bucket_start,

  -- Count how many distinct addresses have ever received this token up to the end of this bucket:
  (
    SELECT 
      COUNT(DISTINCT destination)
    FROM 
      `bigquery-public-data.crypto_solana_mainnet_us.Token Transfers` AS t2
    WHERE
      t2.block_timestamp <= ab.bucket_start
      AND t2.mint = tt.token_mint_address
      AND LOWER(t2.mint) IN (
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
  ) AS holder_count

FROM
  (
    -- Cross‐join each token_mint_address (distinct) with every 12h bucket
    SELECT 
      DISTINCT LOWER(mint) AS token_mint_address
    FROM 
      `bigquery-public-data.crypto_solana_mainnet_us.Token Transfers`
    WHERE 
      LOWER(mint) IN (
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
  ) AS tt
CROSS JOIN
  all_buckets AS ab

ORDER BY
  token_mint_address,
  bucket_start;

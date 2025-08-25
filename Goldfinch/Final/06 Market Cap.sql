-- source-https://defillama.com/protocol/unlocks/goldfinch
WITH
non_circ AS (
  SELECT LOWER(addr) AS owner FROM (
    VALUES
      ('0x0cd73c18c085deb287257ed2307ec713e9af3460'),
      ('0x384860f14b39ccd9c89a73519c70cd5f5394d0a6'),
      ('0x0f306e3f6b2d5ae820d33c284659b29847972d9a'),
      ('0x7766e86584069cf5d1223323d89486e95d9a8c22'),
      ('0xc442b55a082f7d5f8d8dcda3d0eff50f47dd0f82'),
      ('0xe75b8e4da218f21cf0f0ce967ac35276a10aabdb'),
      ('0xbeb28978b2c755155f20fd3d09cb37e300a6981f')
  ) v(addr)
),
days AS (
  SELECT DISTINCT CAST(DATE_TRUNC('day', block_timestamp) AS DATE) AS day
  FROM ethereum.core.fact_token_balances
  WHERE LOWER(contract_address) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b'
),
latest_ts AS (
  SELECT d.day, o.owner, MAX(fb.block_timestamp) AS ts
  FROM days d
  CROSS JOIN non_circ o
  LEFT JOIN ethereum.core.fact_token_balances fb
    ON LOWER(fb.contract_address) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b'
   AND LOWER(fb.user_address)     = o.owner
   AND fb.block_timestamp         < (d.day + 1)
  GROUP BY d.day, o.owner
),
latest_bal AS (
  SELECT
    lt.day,
    lt.owner,
    CAST(fb.balance AS DOUBLE) / 1e18 AS bal,
    ROW_NUMBER() OVER (PARTITION BY lt.day, lt.owner ORDER BY fb.block_number DESC) AS rn
  FROM latest_ts lt
  LEFT JOIN ethereum.core.fact_token_balances fb
    ON LOWER(fb.contract_address) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b'
   AND LOWER(fb.user_address)     = lt.owner
   AND fb.block_timestamp         = lt.ts
),
circ AS (
  SELECT
    day,
    114285714::DOUBLE - COALESCE(SUM(CASE WHEN rn = 1 THEN bal END), 0) AS circulating_gfi_eod
  FROM latest_bal
  GROUP BY day
),
day_ends AS (
  SELECT day, (CAST(day AS TIMESTAMP) + INTERVAL '1 DAY' - INTERVAL '1 SECOND') AS day_end
  FROM days
),
price_eod AS (
  SELECT
    d.day,
    pr.price,
    ROW_NUMBER() OVER (PARTITION BY d.day ORDER BY pr.hour DESC) AS rn
  FROM day_ends d
  JOIN ETHEREUM.PRICE.EZ_PRICES_HOURLY pr
    ON LOWER(pr.token_address) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b'
   AND pr.hour <= d.day_end
   AND COALESCE(pr.is_deprecated, FALSE) = FALSE
   AND COALESCE(pr.is_imputed,   FALSE) = FALSE
   AND COALESCE(pr.is_verified,  TRUE)  = TRUE
)
SELECT
  c.day,
  c.circulating_gfi_eod * p.price AS market_cap_usd_eod
FROM circ c
LEFT JOIN (SELECT day, price FROM price_eod QUALIFY rn = 1) p USING (day)
ORDER BY c.day DESC;
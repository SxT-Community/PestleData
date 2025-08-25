WITH
tv AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM(
      CASE WHEN LOWER(token_in)  = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b' THEN amount_in_usd  ELSE 0 END +
      CASE WHEN LOWER(token_out) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b' THEN amount_out_usd ELSE 0 END
    ) AS gfi_token_usd_volume
  FROM ethereum.defi.ez_dex_swaps
  GROUP BY 1
  HAVING gfi_token_usd_volume > 0
),
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
day_ends AS (
  SELECT
    t.day,
    (t.day::timestamp + INTERVAL '1 DAY' - INTERVAL '1 SECOND') AS day_end
  FROM tv t
),
latest_non_circ AS (
  SELECT
    d.day,
    o.owner,
    CAST(fb.balance AS DOUBLE) / 1e18 AS bal,
    ROW_NUMBER() OVER (PARTITION BY d.day, o.owner ORDER BY fb.block_timestamp DESC) AS rn
  FROM day_ends d
  CROSS JOIN non_circ o
  LEFT JOIN ethereum.core.fact_token_balances fb
    ON LOWER(fb.contract_address) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b'
   AND LOWER(fb.user_address)     = o.owner
   AND fb.block_timestamp        <= d.day_end
),
circ AS (
  SELECT
    d.day,
    114285714::DOUBLE - COALESCE(SUM(CASE WHEN ln.rn = 1 THEN ln.bal END), 0) AS circulating_gfi_eod
  FROM day_ends d
  LEFT JOIN latest_non_circ ln ON ln.day = d.day
  GROUP BY d.day
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
),
mcap AS (
  SELECT
    c.day,
    c.circulating_gfi_eod,
    p.price,
    c.circulating_gfi_eod * p.price AS circulating_market_cap_usd
  FROM circ c
  LEFT JOIN (SELECT day, price FROM price_eod QUALIFY rn = 1) p USING (day)
)
SELECT
  t.day,
  m.circulating_gfi_eod                                  AS circulating_token_supply,
  t.gfi_token_usd_volume                                 AS gfi_token_usd_volume,
  m.circulating_market_cap_usd                           AS circulating_market_cap_usd,
  t.gfi_token_usd_volume / m.circulating_market_cap_usd *100  AS token_turnover_circulating  --precent
FROM tv t
JOIN mcap m USING (day)
ORDER BY t.day DESC;
WITH
price_eod AS (
  SELECT
    CAST(DATE_TRUNC('day', pr.hour) AS DATE) AS day,
    pr.price
  FROM ETHEREUM.PRICE.EZ_PRICES_HOURLY pr
  WHERE LOWER(pr.token_address) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b'
    AND COALESCE(pr.is_deprecated, FALSE) = FALSE
    AND COALESCE(pr.is_imputed,   FALSE) = FALSE
    AND COALESCE(pr.is_verified,  TRUE)  = TRUE
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CAST(DATE_TRUNC('day', pr.hour) AS DATE)
                             ORDER BY pr.hour DESC) = 1
),
latest_treasury AS (
  SELECT
    p.day,
    CAST(fb.balance AS DOUBLE) / 1e18 AS treasury_bal
  FROM price_eod p
  LEFT JOIN ethereum.core.fact_token_balances fb
    ON LOWER(fb.contract_address) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b'
   AND LOWER(fb.user_address)     = '0xbeb28978b2c755155f20fd3d09cb37e300a6981f'
   AND fb.block_timestamp         < (p.day + 1)
  QUALIFY ROW_NUMBER() OVER (PARTITION BY p.day ORDER BY fb.block_timestamp DESC, fb.block_number DESC) = 1
)
SELECT
  p.day,
  (114285714::DOUBLE - COALESCE(t.treasury_bal, 0))                           AS outstanding_supply_eod,
  p.price                                                                      AS price_usd_eod,
  (114285714::DOUBLE - COALESCE(t.treasury_bal, 0)) * p.price                  AS outstanding_fdv_usd_eod
FROM price_eod p
LEFT JOIN latest_treasury t USING (day)
ORDER BY p.day DESC;
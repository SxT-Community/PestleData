--- Inspired by https://dune.com/queries/3274119/5480292

WITH txns AS (
  SELECT
    BLOCK_TIMESTAMP AS time,
    DATE_TRUNC('week', BLOCK_TIMESTAMP) AS week,
    CONTRACT_ADDRESS AS pool_address,  -- using the contract address as the pool identifier
    SYMBOL_IN,
    CAST(AMOUNT_OUT_USD AS DOUBLE) AS amount_usd,
    pool_name,
    tx_hash
  FROM ethereum.defi.ez_dex_swaps
  WHERE platform = 'curve'
    AND BLOCK_TIMESTAMP > DATEADD(day, -365, CURRENT_DATE)
    -- Exclude the glitch period on 2023-07-21 between 19:00 and 19:13 UTC
    AND (BLOCK_TIMESTAMP < TIMESTAMP '2023-07-21 19:00:00' OR BLOCK_TIMESTAMP > TIMESTAMP '2023-07-21 19:13:00')
),

-- Aggregate the USD volume by week for each pool (identified by pool_address and pool_name)
volume2 AS (
  SELECT
    week,
    pool_address,
    pool_name,
    SUM(amount_usd) AS pool_usd_volume
  FROM txns
  GROUP BY week, pool_address, pool_name
)

SELECT *
FROM volume2
WHERE week > DATEADD(day, -365, DATE_TRUNC('week', CURRENT_TIMESTAMP()))
  AND week < CURRENT_TIMESTAMP()
ORDER BY week DESC, pool_name;
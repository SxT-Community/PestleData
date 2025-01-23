
WITH daily_mint AS (
  SELECT
    DATE(block_timestamp) AS date,
    SUM(amount) AS daily_minted
  FROM
    ethereum.core.ez_token_transfers
  WHERE
    LOWER(contract_address) = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52')
    AND LOWER(from_address) = LOWER('0x0000000000000000000000000000000000000000')
  GROUP BY
    DATE(block_timestamp)
),
circulating_supply AS (
  SELECT
    date,
    daily_minted,
    SUM(daily_minted) OVER (
      ORDER BY
        date ASC ROWS BETWEEN UNBOUNDED PRECEDING
        AND CURRENT ROW
    ) AS circulating_supply
  FROM
    daily_mint
),
token_volume AS (
  SELECT
    DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
    sum(
      CASE
        WHEN TOKEN_IN = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52') THEN amount_in
        ELSE 0
      END + CASE
        WHEN TOKEN_OUT = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52') THEN amount_out
        ELSE 0
      END
    ) AS TOKEN_VOLUME,
    sum(
      CASE
        WHEN TOKEN_IN = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52') THEN amount_in_usd
        ELSE 0
      END + CASE
        WHEN TOKEN_OUT = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52') THEN amount_out_usd
        ELSE 0
      END
    ) AS TOKEN_USD_VOLUME
  FROM
    ethereum.defi.ez_dex_swaps
  GROUP BY
    day
  HAVING
    TOKEN_VOLUME > 0
  ORDER BY
    day
),
matched_data AS (
  SELECT
    tv.day,
    tv.TOKEN_VOLUME,
    tv.TOKEN_USD_VOLUME,
    cs.circulating_supply,
    ROW_NUMBER() OVER (
      PARTITION BY tv.day
      ORDER BY
        cs.date DESC
    ) AS row_num
  FROM
    token_volume tv
    LEFT JOIN circulating_supply cs ON cs.date <= tv.day
)
SELECT
  day,
  TOKEN_VOLUME,
  TOKEN_USD_VOLUME,
  circulating_supply / 1e18 as CIRCULATING_TOKEN_SUPPLY,
  TOKEN_VOLUME / 1e9 AS TOKEN_TURNOVER_FULLY_DILUTED,
  TOKEN_VOLUME / (CIRCULATING_TOKEN_SUPPLY / 1e18) AS TOKEN_TURNOVER_CIRCULATING_SUPPLY
FROM
  matched_data
WHERE
  row_num = 1
ORDER BY
  day DESC;
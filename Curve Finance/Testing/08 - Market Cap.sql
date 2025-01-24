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
price_data AS (
    SELECT 
        HOUR,
        PRICE,
        PRICE * 10000000000 AS MARKET_CAP
    FROM ethereum.price.ez_prices_hourly
    WHERE CAST(HOUR AS TIME) = '00:00:00' 
      AND TOKEN_ADDRESS = lower('0xc00e94Cb662C3520282E6f5717214004A7f26888')
),
matched_data AS (
    SELECT 
        p.HOUR,
        p.PRICE,
        p.MARKET_CAP,
        cs.circulating_supply,
        ROW_NUMBER() OVER (PARTITION BY p.HOUR ORDER BY cs.date DESC) AS row_num
    FROM price_data p
    LEFT JOIN circulating_supply cs
    ON cs.date <= DATE(p.HOUR)
)
SELECT 
    HOUR,
    PRICE,
    MARKET_CAP as FULLY_DILUTED_MARKET_CAP,
    circulating_supply/1e18,
    PRICE * circulating_supply/1e18 AS CIRCULATING_SUPPLY_MARKET_CAP
FROM matched_data
WHERE row_num = 1
ORDER BY HOUR;

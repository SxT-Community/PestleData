WITH daily_balances AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        BALANCE,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('day', BLOCK_TIMESTAMP)
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS row_num
    FROM ethereum.core.fact_token_balances
    WHERE lower(USER_ADDRESS) = lower('0x3e40D73EB977Dc6a537aF587D48316feE66E9C8c')
      AND lower(CONTRACT_ADDRESS) = lower('0x5a98fcbea516cf06857215779fd812ca3bef1b32')
),
circulating_supply AS (
    SELECT
        day,
        1000000000e18 - BALANCE AS CIRCULATING_TOKEN_SUPPLY
    FROM daily_balances
    WHERE row_num = 1
),
price_data AS (
    SELECT 
        HOUR,
        PRICE,
        PRICE * 1000000000 AS MARKET_CAP
    FROM ethereum.price.ez_prices_hourly
    WHERE CAST(HOUR AS TIME) = '00:00:00' 
      AND TOKEN_ADDRESS = lower('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32')
),
matched_data AS (
    SELECT 
        p.HOUR,
        p.PRICE,
        p.MARKET_CAP,
        cs.CIRCULATING_TOKEN_SUPPLY,
        ROW_NUMBER() OVER (PARTITION BY p.HOUR ORDER BY cs.day DESC) AS row_num
    FROM price_data p
    LEFT JOIN circulating_supply cs
    ON cs.day <= DATE(p.HOUR)
)
SELECT 
    HOUR,
    PRICE,
    MARKET_CAP as FULLY_DILUTED_MARKET_CAP,
    CIRCULATING_TOKEN_SUPPLY/1e18,
    PRICE * CIRCULATING_TOKEN_SUPPLY/1e18 AS CIRCULATING_SUPPLY_MARKET_CAP
FROM matched_data
WHERE row_num = 1
ORDER BY HOUR
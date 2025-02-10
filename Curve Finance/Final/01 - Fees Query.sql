--- Inspired by this Dune query: https://dune.com/queries/2357882/3862459

WITH daily_fees AS (
    SELECT
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS date,
        SUM(AMOUNT) AS daily_amount
    FROM ethereum.core.ez_token_transfers
    WHERE lower(TO_ADDRESS) IN (
        lower('0xA464e6DCda8AC41e03616F95f4BC98a13b8922Dc'),
        lower('0xD16d5eC345Dd86Fb63C6a9C43c517210F1027914')
    )
    GROUP BY 1
),
price_data AS (
    SELECT 
        DATE(HOUR) AS date,
        AVG(PRICE) AS daily_price
    FROM ethereum.price.ez_prices_hourly
    WHERE lower(TOKEN_ADDRESS) = lower('0xd533a949740bb3306d119cc777fa900ba034cd52')
    GROUP BY 1
)
SELECT 
    f.date,
    f.daily_amount,
    SUM(f.daily_amount) OVER (ORDER BY f.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount,
    f.daily_amount * p.daily_price AS fees_usd,
    SUM(f.daily_amount * p.daily_price) OVER (ORDER BY f.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_usd
FROM daily_fees f
LEFT JOIN price_data p
ON f.date = p.date
ORDER BY f.date;
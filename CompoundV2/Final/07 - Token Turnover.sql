WITH daily_balances AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        BALANCE,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('day', BLOCK_TIMESTAMP)
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS row_num
    FROM ethereum.core.fact_token_balances
    WHERE lower(USER_ADDRESS) = lower('0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B')
      AND lower(CONTRACT_ADDRESS) = lower('0xc00e94Cb662C3520282E6f5717214004A7f26888')
),
circulating_supply AS (
    SELECT
        day,
        10000000000000000000000000 - BALANCE AS CIRCULATING_TOKEN_SUPPLY
    FROM daily_balances
    WHERE row_num = 1
),
token_volume AS (
    SELECT
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        sum(CASE WHEN TOKEN_IN = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN amount_in ELSE 0 END + 
            CASE WHEN TOKEN_OUT = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN amount_out ELSE 0 END) AS TOKEN_VOLUME,
        sum(CASE WHEN TOKEN_IN = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN amount_in_usd ELSE 0 END + 
            CASE WHEN TOKEN_OUT = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN amount_out_usd ELSE 0 END) AS TOKEN_USD_VOLUME
    FROM ethereum.defi.ez_dex_swaps
    GROUP BY day
    HAVING TOKEN_VOLUME > 0
    ORDER BY day
),
matched_data AS (
    SELECT 
        tv.day,
        tv.TOKEN_VOLUME,
        tv.TOKEN_USD_VOLUME,
        cs.CIRCULATING_TOKEN_SUPPLY,
        ROW_NUMBER() OVER (PARTITION BY tv.day ORDER BY cs.day DESC) AS row_num
    FROM token_volume tv
    LEFT JOIN circulating_supply cs
    ON cs.day <= tv.day
)
SELECT 
    day,
    TOKEN_VOLUME,
    TOKEN_USD_VOLUME,
    CIRCULATING_TOKEN_SUPPLY/1e18 as CIRCULATING_TOKEN_SUPPLY,
    TOKEN_VOLUME / 1e9 AS TOKEN_TURNOVER_FULLY_DILUTED,
    TOKEN_VOLUME / (CIRCULATING_TOKEN_SUPPLY/1e18) AS TOKEN_TURNOVER_CIRCULATING_SUPPLY
FROM matched_data
WHERE row_num = 1
ORDER BY day DESC;

WITH daily_balances AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        BALANCE,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('day', BLOCK_TIMESTAMP)
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS row_num
    FROM ethereum.core.fact_token_balances
    WHERE lower(USER_ADDRESS) = lower(‘0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B’)
      AND lower(CONTRACT_ADDRESS) = lower(‘0xc00e94Cb662C3520282E6f5717214004A7f26888’)
)

SELECT
    day,
    BALANCE / 1e18 AS OUTSTANDING_TOKEN_SUPPLY
FROM daily_balances
WHERE row_num = 1
ORDER BY day;

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
)
SELECT
    cast(day as date) as Metric_Date,
    BALANCE / 1e18 AS OUTSTANDING_TOKEN_SUPPLY
FROM daily_balances
WHERE row_num = 1
ORDER BY 1
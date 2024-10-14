WITH daily_snapshots AS (
    SELECT 
        BLOCK_TIMESTAMP,
        BALANCE,
        CONTRACT_ADDRESS,
        USER_ADDRESS,
        ROW_NUMBER() OVER (
            PARTITION BY CONTRACT_ADDRESS, 
                         DATE_TRUNC('day', BLOCK_TIMESTAMP)
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS row_num
    FROM ethereum.core.fact_token_balances
    WHERE (
        lower(CONTRACT_ADDRESS) = lower('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
        --STETH
        OR lower(CONTRACT_ADDRESS) = lower('0x7d1afa7b718fb893db30a3abc0cfc608aacfebb0')
        --MATIC
    )
    AND lower(USER_ADDRESS) = lower('0x3e40D73EB977Dc6a537aF587D48316feE66E9C8c')
    --LIDO Treasury address
)
SELECT cast(BLOCK_TIMESTAMP as date) as BLOCK_DATE, 
BALANCE, CONTRACT_ADDRESS, USER_ADDRESS
FROM daily_snapshots
WHERE row_num = 1
ORDER BY 1

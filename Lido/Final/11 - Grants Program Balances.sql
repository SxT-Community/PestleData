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
      lower(CONTRACT_ADDRESS) = lower('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        --DAI
        OR lower(CONTRACT_ADDRESS) = lower('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32')
        --LIDO
    )
    AND lower(USER_ADDRESS) = lower('0x12a43b049A7D330cB8aEAB5113032D18AE9a9030')
    --LIDO Lego Wallet
)
SELECT BLOCK_TIMESTAMP, BALANCE, CONTRACT_ADDRESS, USER_ADDRESS
FROM daily_snapshots
WHERE row_num = 1
ORDER BY BLOCK_TIMESTAMP;

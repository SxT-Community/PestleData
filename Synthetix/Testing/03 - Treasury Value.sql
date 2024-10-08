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
        lower(CONTRACT_ADDRESS) = lower('0x99F4176EE457afedFfCB1839c7aB7A030a5e4A92')
        --Synth ETH
        OR lower(CONTRACT_ADDRESS) = lower('0x4c9EDD5852cd905f086C759E8383e09bff1E68B3')
        --usdE 
        OR lower(CONTRACT_ADDRESS) = lower('0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48')
        --USDC
        OR lower(CONTRACT_ADDRESS) = lower('0x57Ab1ec28D129707052df4dF418D58a2D46d5f51')
        --sUSD
        OR lower(CONTRACT_ADDRESS) = lower('0x7f50786A0b15723D741727882ee99a0BF34e3466')
        --sdCRV
        OR lower(CONTRACT_ADDRESS) = lower('0xba100000625a3754423978a60c9317c58a424e3D')
        --BAL
        OR lower(CONTRACT_ADDRESS) = lower('0x5A98FcBEA516Cf06857215779Fd812CA3beF1B32')
        --LIDO
        OR lower(CONTRACT_ADDRESS) = lower('0x5aFE3855358E112B5647B952709E6165e1c1eEEe')
        --SAFE
        OR lower(CONTRACT_ADDRESS) = lower('0x4e3FBD56CD56c3e72c1403e103b45Db9da5B9D2B')
        --CVX
        OR lower(CONTRACT_ADDRESS) = lower('0x31c8EAcBFFdD875c74b94b077895Bd78CF1E64A3')
        --RAD
    )
    AND lower(USER_ADDRESS) = lower(‘0x99F4176EE457afedFfCB1839c7aB7A030a5e4A92’)
    --LIDO Treasury address
)
SELECT BLOCK_TIMESTAMP, BALANCE, CONTRACT_ADDRESS, USER_ADDRESS
FROM daily_snapshots
WHERE row_num = 1
ORDER BY BLOCK_TIMESTAMP;

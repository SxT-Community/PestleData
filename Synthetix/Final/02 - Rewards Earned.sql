WITH ethereum_rewards AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        CAST(decoded_log['amount']::STRING AS NUMERIC) AS ETHEREUM_SNX_REWARDS,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('day', BLOCK_TIMESTAMP)
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS row_num
    FROM ethereum.core.ez_decoded_event_logs
    WHERE 
        lower(CONTRACT_ADDRESS) = lower('0x94433f0DA8B5bfb473Ea8cd7ad10D9c8aef4aB7b')
        AND lower(event_name) = 'rewardsdistributed'
),
optimism_rewards AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        CAST(decoded_log['amount']::STRING AS NUMERIC) AS OPTIMISM_SNX_REWARDS,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('day', BLOCK_TIMESTAMP)
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS row_num
    FROM optimism.core.ez_decoded_event_logs
    WHERE 
        lower(CONTRACT_ADDRESS) = lower('0x5d9187630E99dBce4BcAB8733B76757f7F44aA2e')
        AND lower(event_name) = 'rewardsdistributed'
)
SELECT 
    COALESCE(e.day, o.day) AS BLOCK_TIMESTAMP,
    SUM(e.ETHEREUM_SNX_REWARDS) OVER (ORDER BY COALESCE(e.day, o.day) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ETHEREUM_SNX_REWARDS_RUNNING_TOTAL,
    SUM(o.OPTIMISM_SNX_REWARDS) OVER (ORDER BY COALESCE(e.day, o.day) ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS OPTIMISM_SNX_REWARDS_RUNNING_TOTAL
FROM ethereum_rewards e
FULL OUTER JOIN optimism_rewards o
    ON e.day = o.day
WHERE e.row_num = 1 OR o.row_num = 1
ORDER BY BLOCK_TIMESTAMP;

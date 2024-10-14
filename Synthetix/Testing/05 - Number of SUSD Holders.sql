-- Synth USD on base
-- https://basescan.org/token/0x09d51516F38980035153a554c26Df3C6f51a23C3
-- Diff C-96 : O-112

-- Step 1: Get all token transfers with amount changes
WITH token_transfers AS (
    SELECT
        BLOCK_TIMESTAMP,
        FROM_ADDRESS AS address,
        -AMOUNT AS amount_change
    FROM
        base.core.ez_token_transfers
    WHERE
        contract_address = LOWER('0x09d51516F38980035153a554c26Df3C6f51a23C3')

    UNION ALL

    SELECT
        BLOCK_TIMESTAMP,
        TO_ADDRESS AS address,
        AMOUNT AS amount_change
    FROM
        base.core.ez_token_transfers
    WHERE
        contract_address = LOWER('0x09d51516F38980035153a554c26Df3C6f51a23C3')
),

-- Step 2: Calculate cumulative balances for each address over time
cumulative_balances AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        SUM(amount_change) OVER (
            PARTITION BY address
            ORDER BY BLOCK_TIMESTAMP
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_balance
    FROM
        token_transfers
),

-- Step 3: Assign balance flags
balance_flags AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        cumulative_balance,
        CASE WHEN cumulative_balance > 0 THEN 1 ELSE 0 END AS balance_flag
    FROM
        cumulative_balances
),

-- Step 4: Identify balance transitions
balance_transitions AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        cumulative_balance,
        balance_flag,
        LAG(balance_flag) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) AS prev_balance_flag,
        CASE
            WHEN balance_flag = 1 AND (LAG(balance_flag) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) = 0 OR LAG(balance_flag) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) IS NULL)
                THEN 1 -- Start of positive balance period
            WHEN balance_flag = 0 AND LAG(balance_flag) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) = 1
                THEN -1 -- End of positive balance period
            ELSE 0
        END AS balance_change
    FROM
        balance_flags
),

-- Step 5: Assign period IDs
balance_periods AS (
    SELECT
        *,
        SUM(CASE WHEN balance_change = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) AS period_id
    FROM
        balance_transitions
),

-- Step 6: Get positive balance periods
positive_balance_periods AS (
    SELECT
        address,
        period_id,
        MIN(CASE WHEN balance_change = 1 THEN BLOCK_TIMESTAMP END) AS start_time,
        MAX(CASE WHEN balance_change = -1 THEN BLOCK_TIMESTAMP END) AS end_time
    FROM
        balance_periods
    GROUP BY
        address,
        period_id
),

-- Step 7: Get max timestamp
max_timestamp AS (
    SELECT MAX(BLOCK_TIMESTAMP) AS max_timestamp FROM cumulative_balances
),

-- Step 8: Final positive balance intervals
positive_balance_intervals AS (
    SELECT
        address,
        start_time,
        COALESCE(end_time, (SELECT max_timestamp FROM max_timestamp)) AS end_time
    FROM
        positive_balance_periods
),

-- Step 9: Generate calendar dates
date_range AS (
    SELECT
        DATE_TRUNC('day', MIN(BLOCK_TIMESTAMP)) AS min_day,
        DATE_TRUNC('day', MAX(BLOCK_TIMESTAMP)) AS max_day
    FROM
        token_transfers
),

calendar AS (
    SELECT
        DATEADD(
            day,
            seq_day,
            (SELECT min_day FROM date_range)
        ) AS day
    FROM (
        SELECT
            SEQ4() AS seq_day
        FROM
            TABLE(GENERATOR(ROWCOUNT => 2000)) -- Adjust ROWCOUNT as needed
    )
    WHERE
        seq_day <= DATEDIFF(
            'day',
            (SELECT min_day FROM date_range),
            (SELECT max_day FROM date_range)
        )
),

-- Step 10: Count cumulative holders per day
daily_holders AS (
    SELECT
        c.day,
        COUNT(DISTINCT pbi.address) AS cumulative_holders
    FROM
        calendar c
    LEFT JOIN
        positive_balance_intervals pbi
    ON
        c.day >= DATE_TRUNC('day', pbi.start_time) AND c.day <= DATE_TRUNC('day', pbi.end_time)
    GROUP BY
        c.day
    ORDER BY
        c.day
)

-- Final result
SELECT
    day,
    cumulative_holders
FROM
    daily_holders;
ORDER BY 
    day
;


-- Synth USD on Optimism
-- https://optimistic.etherscan.io/token/0x8c6f28f2f1a3c87f0f938b96d27520d9751ec8d9#code
-- **Diff C-193549 : O-123222


-- Step 1: Get all token transfers with amount changes
WITH token_transfers AS (
    SELECT
        BLOCK_TIMESTAMP,
        FROM_ADDRESS AS address,
        -AMOUNT AS amount_change
    FROM
        optimism.core.ez_token_transfers
    WHERE
        contract_address = LOWER('0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9')

    UNION ALL

    SELECT
        BLOCK_TIMESTAMP,
        TO_ADDRESS AS address,
        AMOUNT AS amount_change
    FROM
        optimism.core.ez_token_transfers
    WHERE
        contract_address = LOWER('0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9')
),

-- Step 2: Calculate cumulative balances for each address over time
cumulative_balances AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        SUM(amount_change) OVER (
            PARTITION BY address
            ORDER BY BLOCK_TIMESTAMP
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_balance
    FROM
        token_transfers
),

-- Step 3: Assign balance flags
balance_flags AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        cumulative_balance,
        CASE WHEN cumulative_balance > 0 THEN 1 ELSE 0 END AS balance_flag
    FROM
        cumulative_balances
),

-- Step 4: Identify balance transitions
balance_transitions AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        cumulative_balance,
        balance_flag,
        LAG(balance_flag) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) AS prev_balance_flag,
        CASE
            WHEN balance_flag = 1 AND (LAG(balance_flag) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) = 0 OR LAG(balance_flag) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) IS NULL)
                THEN 1 -- Start of positive balance period
            WHEN balance_flag = 0 AND LAG(balance_flag) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) = 1
                THEN -1 -- End of positive balance period
            ELSE 0
        END AS balance_change
    FROM
        balance_flags
),

-- Step 5: Assign period IDs
balance_periods AS (
    SELECT
        *,
        SUM(CASE WHEN balance_change = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) AS period_id
    FROM
        balance_transitions
),

-- Step 6: Get positive balance periods
positive_balance_periods AS (
    SELECT
        address,
        period_id,
        MIN(CASE WHEN balance_change = 1 THEN BLOCK_TIMESTAMP END) AS start_time,
        MAX(CASE WHEN balance_change = -1 THEN BLOCK_TIMESTAMP END) AS end_time
    FROM
        balance_periods
    GROUP BY
        address,
        period_id
),

-- Step 7: Get max timestamp
max_timestamp AS (
    SELECT MAX(BLOCK_TIMESTAMP) AS max_timestamp FROM cumulative_balances
),

-- Step 8: Final positive balance intervals
positive_balance_intervals AS (
    SELECT
        address,
        start_time,
        COALESCE(end_time, (SELECT max_timestamp FROM max_timestamp)) AS end_time
    FROM
        positive_balance_periods
),

-- Step 9: Generate calendar dates
date_range AS (
    SELECT
        DATE_TRUNC('day', MIN(BLOCK_TIMESTAMP)) AS min_day,
        DATE_TRUNC('day', MAX(BLOCK_TIMESTAMP)) AS max_day
    FROM
        token_transfers
),

calendar AS (
    SELECT
        DATEADD(
            day,
            seq_day,
            (SELECT min_day FROM date_range)
        ) AS day
    FROM (
        SELECT
            SEQ4() AS seq_day
        FROM
            TABLE(GENERATOR(ROWCOUNT => 2000)) -- Adjust ROWCOUNT as needed
    )
    WHERE
        seq_day <= DATEDIFF(
            'day',
            (SELECT min_day FROM date_range),
            (SELECT max_day FROM date_range)
        )
),

-- Step 10: Count cumulative holders per day
daily_holders AS (
    SELECT
        c.day,
        COUNT(DISTINCT pbi.address) AS cumulative_holders
    FROM
        calendar c
    LEFT JOIN
        positive_balance_intervals pbi
    ON
        c.day >= DATE_TRUNC('day', pbi.start_time) AND c.day <= DATE_TRUNC('day', pbi.end_time)
    GROUP BY
        c.day
    ORDER BY
        c.day
)

-- Final result
SELECT
    day,
    cumulative_holders
FROM
    daily_holders;
ORDER BY 
    day
;



-- Synth USD on Ethereum
-- https://etherscan.io/token/0x57Ab1ec28D129707052df4dF418D58a2D46d5f51
-- Diff C-14759 : O-14783

WITH balance_history AS (
    SELECT 
        USER_ADDRESS,
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        BALANCE,
        ROW_NUMBER() OVER (
            PARTITION BY USER_ADDRESS, DATE_TRUNC('day', BLOCK_TIMESTAMP)
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS row_num
    FROM ethereum.core.fact_token_balances
    WHERE CONTRACT_ADDRESS = LOWER('0x57Ab1ec28D129707052df4dF418D58a2D46d5f51') 
),
latest_balances_per_day AS (
    SELECT
        USER_ADDRESS,
        day,
        BALANCE
    FROM balance_history
    WHERE row_num = 1  -- Get the latest balance for each USER_ADDRESS per day
),
balance_events AS (
    SELECT
        USER_ADDRESS,
        day,
        CASE
            WHEN BALANCE > 0 AND NVL(LAG(BALANCE) OVER (PARTITION BY USER_ADDRESS ORDER BY day), 0) = 0 THEN 1
            WHEN BALANCE = 0 AND NVL(LAG(BALANCE) OVER (PARTITION BY USER_ADDRESS ORDER BY day), 0) > 0 THEN -1
            ELSE 0
        END AS balance_change
    FROM latest_balances_per_day
),
filtered_events AS (
    SELECT
        day,
        balance_change
    FROM balance_events
    WHERE balance_change <> 0
),
daily_changes AS (
    SELECT
        day,
        SUM(balance_change) AS net_change
    FROM filtered_events
    GROUP BY day
),
min_max_days AS (
    SELECT
        MIN(day) AS min_day,
        MAX(day) AS max_day
    FROM daily_changes
),
date_range AS (
    SELECT min_day AS day FROM min_max_days
    UNION ALL
    SELECT DATEADD('day', 1, day)
    FROM date_range
    WHERE day < (SELECT max_day FROM min_max_days)
),
daily_net_changes AS (
    SELECT
        dr.day,
        COALESCE(dc.net_change, 0) AS net_change
    FROM date_range dr
    LEFT JOIN daily_changes dc ON dr.day = dc.day
),
cumulative_token_holders AS (
    SELECT
        day,
        SUM(net_change) OVER (ORDER BY day) AS unique_token_holders
    FROM daily_net_changes
)
SELECT
    day,
    unique_token_holders
FROM cumulative_token_holders
ORDER BY day;

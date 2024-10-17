
-- BASE
WITH token_transfers_base AS (
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

cumulative_balances_base AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        SUM(amount_change) OVER (
            PARTITION BY address
            ORDER BY BLOCK_TIMESTAMP
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_balance
    FROM
        token_transfers_base
),

balance_flags_base AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        cumulative_balance,
        CASE WHEN cumulative_balance > 0 THEN 1 ELSE 0 END AS balance_flag
    FROM
        cumulative_balances_base
),

balance_transitions_base AS (
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
        balance_flags_base
),

balance_periods_base AS (
    SELECT
        *,
        SUM(CASE WHEN balance_change = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) AS period_id
    FROM
        balance_transitions_base
),

positive_balance_periods_base AS (
    SELECT
        address,
        period_id,
        MIN(CASE WHEN balance_change = 1 THEN BLOCK_TIMESTAMP END) AS start_time,
        MAX(CASE WHEN balance_change = -1 THEN BLOCK_TIMESTAMP END) AS end_time
    FROM
        balance_periods_base
    GROUP BY
        address,
        period_id
),

max_timestamp_base AS (
    SELECT MAX(BLOCK_TIMESTAMP) AS max_timestamp_base FROM cumulative_balances_base
),

positive_balance_intervals_base AS (
    SELECT
        address,
        start_time,
        COALESCE(end_time, (SELECT max_timestamp_base FROM max_timestamp_base)) AS end_time
    FROM
        positive_balance_periods_base
),

date_range_base AS (
    SELECT
        DATE_TRUNC('day', MIN(BLOCK_TIMESTAMP)) AS min_day,
        DATE_TRUNC('day', MAX(BLOCK_TIMESTAMP)) AS max_day
    FROM
        token_transfers_base
),

calendar_base AS (
    SELECT
        DATEADD(
            day,
            seq_day,
            (SELECT min_day FROM date_range_base)
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
            (SELECT min_day FROM date_range_base),
            (SELECT max_day FROM date_range_base)
        )
),

daily_holders_base AS (
    SELECT
        c.day,
        COUNT(DISTINCT pbi.address) AS cumulative_holders
    FROM
        calendar_base c
    LEFT JOIN
        positive_balance_intervals_base pbi
    ON
        c.day >= DATE_TRUNC('day', pbi.start_time) AND c.day <= DATE_TRUNC('day', pbi.end_time)
    GROUP BY
        c.day
    ORDER BY
        c.day
),

-- OPTIMISM

token_transfers_opt AS (
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

cumulative_balances_opt AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        SUM(amount_change) OVER (
            PARTITION BY address
            ORDER BY BLOCK_TIMESTAMP
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_balance
    FROM
        token_transfers_opt
),

balance_flags_opt AS (
    SELECT
        address,
        BLOCK_TIMESTAMP,
        cumulative_balance,
        CASE WHEN cumulative_balance > 0 THEN 1 ELSE 0 END AS balance_flag
    FROM
        cumulative_balances_opt
),

balance_transitions_opt AS (
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
        balance_flags_opt
),

balance_periods_opt AS (
    SELECT
        *,
        SUM(CASE WHEN balance_change = 1 THEN 1 ELSE 0 END) OVER (PARTITION BY address ORDER BY BLOCK_TIMESTAMP) AS period_id
    FROM
        balance_transitions_opt
),

positive_balance_periods_opt AS (
    SELECT
        address,
        period_id,
        MIN(CASE WHEN balance_change = 1 THEN BLOCK_TIMESTAMP END) AS start_time,
        MAX(CASE WHEN balance_change = -1 THEN BLOCK_TIMESTAMP END) AS end_time
    FROM
        balance_periods_opt
    GROUP BY
        address,
        period_id
),

max_timestamp_opt AS (
    SELECT MAX(BLOCK_TIMESTAMP) AS max_timestamp_opt FROM cumulative_balances_opt
),

positive_balance_intervals_opt AS (
    SELECT
        address,
        start_time,
        COALESCE(end_time, (SELECT max_timestamp_opt FROM max_timestamp_opt)) AS end_time
    FROM
        positive_balance_periods_opt
),

date_range_opt AS (
    SELECT
        DATE_TRUNC('day', MIN(BLOCK_TIMESTAMP)) AS min_day,
        DATE_TRUNC('day', MAX(BLOCK_TIMESTAMP)) AS max_day
    FROM
        token_transfers_opt
),

calendar_opt AS (
    SELECT
        DATEADD(
            day,
            seq_day,
            (SELECT min_day FROM date_range_opt)
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
            (SELECT min_day FROM date_range_opt),
            (SELECT max_day FROM date_range_opt)
        )
),

daily_holders_opt AS (
    SELECT
        c.day,
        COUNT(DISTINCT pbi.address) AS cumulative_holders
    FROM
        calendar_opt c
    LEFT JOIN
        positive_balance_intervals_opt pbi
    ON
        c.day >= DATE_TRUNC('day', pbi.start_time) AND c.day <= DATE_TRUNC('day', pbi.end_time)
    GROUP BY
        c.day
    ORDER BY
        c.day
),

-- ETHEREUM

balance_history_eth AS (
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
latest_balances_per_day_eth AS (
    SELECT
        USER_ADDRESS,
        day,
        BALANCE
    FROM balance_history_eth
    WHERE row_num = 1  -- Get the latest balance for each USER_ADDRESS per day
),
balance_events_eth AS (
    SELECT
        USER_ADDRESS,
        day,
        CASE
            WHEN BALANCE > 0 AND NVL(LAG(BALANCE) OVER (PARTITION BY USER_ADDRESS ORDER BY day), 0) = 0 THEN 1
            WHEN BALANCE = 0 AND NVL(LAG(BALANCE) OVER (PARTITION BY USER_ADDRESS ORDER BY day), 0) > 0 THEN -1
            ELSE 0
        END AS balance_change
    FROM latest_balances_per_day_eth
),
filtered_events_eth AS (
    SELECT
        day,
        balance_change
    FROM balance_events_eth
    WHERE balance_change <> 0
),
daily_changes_eth AS (
    SELECT
        day,
        SUM(balance_change) AS net_change
    FROM filtered_events_eth
    GROUP BY day
),
min_max_days_eth AS (
    SELECT
        MIN(day) AS min_day,
        MAX(day) AS max_day
    FROM daily_changes_eth
),
date_range_eth AS (
    SELECT min_day AS day FROM min_max_days_eth
    UNION ALL
    SELECT DATEADD('day', 1, day)
    FROM date_range_eth
    WHERE day < (SELECT max_day FROM min_max_days_eth)
),
daily_net_changes_eth AS (
    SELECT
        dr.day,
        COALESCE(dc.net_change, 0) AS net_change
    FROM date_range_eth dr
    LEFT JOIN daily_changes_eth dc ON dr.day = dc.day
),
cumulative_token_holders_eth AS (
    SELECT
        day,
        SUM(net_change) OVER (ORDER BY day) AS unique_token_holders
    FROM daily_net_changes_eth
)


-- Final result

SELECT
    'ETHEREUM' AS blockchain,
    day,
    unique_token_holders
FROM cumulative_token_holders_eth

 UNION ALL 

SELECT
    'OPTIMISM' AS blockchain,
    day,
    cumulative_holders
FROM
    daily_holders_opt;

UNION ALL

SELECT
    'BASE' AS blockchain,
    day,
    cumulative_holders
FROM
    daily_holders_base
ORDER BY 
    day
;
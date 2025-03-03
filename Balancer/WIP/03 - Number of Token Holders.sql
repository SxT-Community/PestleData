-- https://dune.com/queries/4464849

-- Modified query works on Dune but not on Flipside
-- yet to figure out the tables and columns
WITH combined_transactions AS (
    SELECT
        tx_from AS user_address,
        block_times AS event_time
    FROM balancer.trades
    
    UNION ALL
    
    SELECT
        "from" AS user_address,
        evt_block_time AS event_time
    FROM balancer.transfers_bpt
    
    UNION ALL
    
    SELECT
        "to" AS user_address,
        evt_block_time AS event_time
    FROM balancer.transfers_bpt
),

user_first_interaction AS (
    SELECT
        user_address,
        MIN(event_time) AS first_interaction_time
    FROM combined_transactions
    GROUP BY user_address
),

daily_new_wallets AS (
    SELECT
        DATE_TRUNC('day', first_interaction_time) AS day,
        COUNT(DISTINCT user_address) AS new_users
    FROM user_first_interaction
    GROUP BY DATE_TRUNC('day', first_interaction_time)
)

SELECT
    day,
    new_users,
    SUM(new_users) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS new_users_with_previous_day

FROM daily_new_wallets
ORDER BY day;

/*
Replicate LIDO query, but will need to account for multiple chains. 
SNX on Ethereum address: 0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f and 
SNX on Optimism: 0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4.
*/

ON Ethereum
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
    WHERE CONTRACT_ADDRESS = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f') 
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


On Optimism


WITH balance_history AS (
  SELECT
    DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
    user_address,
    SUM(
      CASE
        WHEN direction = 'in' THEN RAW_AMOUNT
        ELSE 0
      END
    ) AS total_in,
    SUM(
      CASE
        WHEN direction = 'out' THEN RAW_AMOUNT
        ELSE 0
      END
    ) AS total_out,
    SUM(
      CASE
        WHEN direction = 'in' THEN RAW_AMOUNT
        ELSE 0
      END
    ) - SUM(
      CASE
        WHEN direction = 'out' THEN RAW_AMOUNT
        ELSE 0
      END
    ) AS BALANCE
  FROM
    (
      SELECT
        BLOCK_TIMESTAMP,
        FROM_ADDRESS AS user_address,
        RAW_AMOUNT,
        'out' AS direction
      FROM
        optimism.core.fact_token_transfers
      WHERE
        contract_address = LOWER('0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4')
      UNION
      ALL
      SELECT
        BLOCK_TIMESTAMP,
        TO_ADDRESS AS user_address,
        RAW_AMOUNT,
        'in' AS direction
      FROM
        optimism.core.fact_token_transfers
      WHERE
        contract_address = LOWER('0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4')
    ) AS combined
  GROUP BY
    DATE_TRUNC('day', BLOCK_TIMESTAMP),
    user_address
  ORDER BY
    day,
    user_address
),
balance_events AS (
  SELECT
    USER_ADDRESS,
    DAY,
    CASE
      WHEN BALANCE > 0
      AND NVL(
        LAG(BALANCE) OVER (
          PARTITION BY USER_ADDRESS
          ORDER BY
            day
        ),
        0
      ) = 0 THEN 1
      WHEN BALANCE = 0
      AND NVL(
        LAG(BALANCE) OVER (
          PARTITION BY USER_ADDRESS
          ORDER BY
            day
        ),
        0
      ) > 0 THEN -1
      ELSE 0
    END AS balance_change
  FROM
    balance_history
),
filtered_events AS (
  SELECT
    day,
    balance_change
  FROM
    balance_events
  WHERE
    balance_change <> 0
),
daily_changes AS (
  SELECT
    day,
    SUM(balance_change) AS net_change
  FROM
    filtered_events
  GROUP BY
    day
),
min_max_days AS (
  SELECT
    MIN(day) AS min_day,
    MAX(day) AS max_day
  FROM
    daily_changes
),
date_range AS (
  SELECT
    min_day AS day
  FROM
    min_max_days
  UNION
  ALL
  SELECT
    DATEADD('day', 1, day)
  FROM
    date_range
  WHERE
    day < (
      SELECT
        max_day
      FROM
        min_max_days
    )
),
daily_net_changes AS (
  SELECT
    dr.day,
    COALESCE(dc.net_change, 0) AS net_change
  FROM
    date_range dr
    LEFT JOIN daily_changes dc ON dr.day = dc.day
),
cumulative_token_holders AS (
  SELECT
    day,
    SUM(net_change) OVER (
      ORDER BY
        day
    ) AS unique_token_holders
  FROM
    daily_net_changes
)
SELECT
  day,
  unique_token_holders
FROM
  cumulative_token_holders
ORDER BY
  day;


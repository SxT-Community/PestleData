WITH
params AS (
  SELECT TO_DATE('2021-08-19') AS start_day, CURRENT_DATE() AS end_day
),

tranched_pools AS (
  SELECT DISTINCT LOWER(decoded_log:pool::string) AS pool_address
  FROM ethereum.core.ez_decoded_event_logs
  WHERE LOWER(contract_address) = '0xd20508e1e971b80ee172c73517905bfffcbd87f9' 
    AND event_name = 'PoolCreated'
    AND tx_succeeded = 'TRUE'
    AND block_number >= 13097274
),

owners AS (
  SELECT '0x8481a6ebaf5c7dabc3f7e09e44a89531fd31f822' AS owner
  UNION ALL
  SELECT pool_address FROM tranched_pools
),

last_before AS (
  SELECT owner, bal
  FROM (
    SELECT
      LOWER(fb.user_address) AS owner,
      fb.block_timestamp,
      CAST(fb.balance AS FLOAT) / 1e6 AS bal,                
      ROW_NUMBER() OVER (
        PARTITION BY LOWER(fb.user_address)
        ORDER BY fb.block_timestamp DESC
      ) AS rn
    FROM ethereum.core.fact_token_balances fb
    WHERE LOWER(fb.contract_address) = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'  
      AND LOWER(fb.user_address) IN (SELECT owner FROM owners)
      AND fb.block_timestamp < (SELECT start_day::timestamp_ntz FROM params)
  )
  WHERE rn = 1
),

anchors AS (
  SELECT
    o.owner,
    (SELECT start_day::timestamp_ntz FROM params) - INTERVAL '1 SECOND' AS ts,
    COALESCE(lb.bal, 0.0) AS bal
  FROM owners o
  LEFT JOIN last_before lb ON lb.owner = o.owner
),

in_window_changes AS (
  SELECT
    LOWER(fb.user_address) AS owner,
    fb.block_timestamp     AS ts,
    CAST(fb.balance AS FLOAT) / 1e6 AS bal                 
  FROM ethereum.core.fact_token_balances fb
  WHERE LOWER(fb.contract_address) = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48'
    AND LOWER(fb.user_address) IN (SELECT owner FROM owners)
    AND fb.block_timestamp >= (SELECT start_day::timestamp_ntz FROM params)
    AND fb.block_timestamp <  ((SELECT end_day::timestamp_ntz FROM params) + INTERVAL '1 DAY')
),

balance_points AS (
  SELECT * FROM anchors
  UNION ALL
  SELECT * FROM in_window_changes
),

intervals AS (
  SELECT
    owner,
    ts AS start_ts,
    LEAD(ts) OVER (PARTITION BY owner ORDER BY ts) AS end_ts,
    bal
  FROM balance_points
),

days AS (
  SELECT DATEADD(day, SEQ4(), (SELECT start_day FROM params)) AS day
  FROM TABLE(GENERATOR(ROWCOUNT => 5000))      
),

day_ends AS (
  SELECT
    day,
    (day::timestamp_ntz + INTERVAL '1 DAY' - INTERVAL '1 SECOND') AS day_end
  FROM days
  WHERE day <= (SELECT end_day FROM params)     
)

SELECT
  d.day,
  COALESCE(SUM(i.bal), 0) AS goldfinch_base_tvl_usdc
FROM day_ends d
LEFT JOIN intervals i
  ON d.day_end >= i.start_ts
 AND (i.end_ts IS NULL OR d.day_end < i.end_ts)
GROUP BY d.day
ORDER BY d.day desc;
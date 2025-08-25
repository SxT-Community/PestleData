WITH
  last_before AS (
    SELECT owner, bal_raw
    FROM (
      SELECT
        LOWER(user_address) AS owner,
        balance             AS bal_raw,
        ROW_NUMBER() OVER (
          PARTITION BY LOWER(user_address)
          ORDER BY block_timestamp DESC
        ) AS rn
      FROM ethereum.core.fact_token_balances
      WHERE LOWER(contract_address) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b' 
        AND block_timestamp < DATE '2021-08-19'
    )
    WHERE rn = 1
  ),
  points AS (
    SELECT
      lb.owner,
      (CAST(DATE '2021-08-19' AS TIMESTAMP) - INTERVAL '1 SECOND') AS ts,
      lb.bal_raw
    FROM last_before lb
    UNION ALL
    SELECT
      LOWER(user_address) AS owner,
      block_timestamp     AS ts,
      balance             AS bal_raw
    FROM ethereum.core.fact_token_balances
    WHERE LOWER(contract_address) = '0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b'
      AND block_timestamp >= DATE '2021-08-19'
      AND block_timestamp < (CURRENT_DATE() + 1)
  ),
  intervals AS (
    SELECT
      owner,
      ts AS start_ts,
      LEAD(ts) OVER (PARTITION BY owner ORDER BY ts) AS end_ts,
      bal_raw
    FROM points
  ),
  day_ends AS (
    SELECT
      day,
      (CAST(day AS TIMESTAMP) + INTERVAL '1 DAY' - INTERVAL '1 SECOND') AS day_end
    FROM (
      SELECT DATEADD(day, SEQ4(), DATE '2021-08-19') AS day
      FROM TABLE(GENERATOR(ROWCOUNT => 5000))
    )
    WHERE day <= CURRENT_DATE()
  )
SELECT
  d.day,
  COUNT_IF(i.owner IS NOT NULL AND i.bal_raw > 0) AS gfi_holders_eod
FROM day_ends d
LEFT JOIN intervals i
  ON d.day_end >= i.start_ts
 AND (i.end_ts IS NULL OR d.day_end < i.end_ts)
GROUP BY d.day
ORDER BY d.day DESC;
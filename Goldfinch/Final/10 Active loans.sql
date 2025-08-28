WITH
pools AS (
  SELECT DISTINCT LOWER(decoded_log:pool::string) AS loan
  FROM ethereum.core.ez_decoded_event_logs
  WHERE tx_succeeded = 'TRUE'
    AND LOWER(contract_address) = '0xd20508e1e971b80ee172c73517905bfffcbd87f9'
    AND event_name = 'PoolCreated'
    AND block_number >= 13097274
),
flows AS (
  SELECT
    DATE_TRUNC('day', block_timestamp)                AS day,
    LOWER(contract_address)                           AS loan,
    SUM(CASE
          WHEN event_name = 'DrawdownMade'   THEN CAST(decoded_log:amount AS DOUBLE)/1e6
          WHEN event_name = 'PaymentApplied' THEN -CAST(decoded_log:principalAmount AS DOUBLE)/1e6
          ELSE 0
        END)                                          AS net_usdc
  FROM ethereum.core.ez_decoded_event_logs
  WHERE tx_succeeded = 'TRUE'
    AND event_name IN ('DrawdownMade','PaymentApplied')
    AND LOWER(contract_address) IN (SELECT loan FROM pools)
  GROUP BY 1,2
),
days AS (
  SELECT DATEADD(day, SEQ4(), (SELECT MIN(day) FROM flows)) AS day
  FROM TABLE(GENERATOR(ROWCOUNT => 5000))
  WHERE day <= (SELECT MAX(day) FROM flows)
),
running AS (
  SELECT
    d.day,
    p.loan,
    SUM(COALESCE(f.net_usdc, 0))
      OVER (PARTITION BY p.loan ORDER BY d.day ROWS UNBOUNDED PRECEDING) AS outstanding_usdc
  FROM pools p
  CROSS JOIN days d
  LEFT JOIN flows f
    ON f.loan = p.loan AND f.day = d.day
)
SELECT
  day,
  SUM(CASE WHEN outstanding_usdc > 0 THEN outstanding_usdc ELSE 0 END) AS borrowed_usd_eod
FROM running
GROUP BY day
ORDER BY day desc;
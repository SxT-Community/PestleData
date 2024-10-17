WITH combined_mints AS (
  SELECT
    LOWER(decoded_log:account) AS account,
    CAST(decoded_log:amount AS DECIMAL) AS amount,
    CAST(block_timestamp AS TIMESTAMP) AS time,
    'L2' AS layer
  FROM
    optimism.core.ez_decoded_event_logs
  WHERE
    contract_address = LOWER('0x45c55BF488D3Cb8640f12F63CbeDC027E8261E79')
    AND lower(event_name) = 'mint'
  
  UNION ALL
  
  SELECT
    LOWER(decoded_log:account) AS account,
    CAST(decoded_log:amount AS DECIMAL) AS amount,
    CAST(block_timestamp AS TIMESTAMP) AS time,
    'L1' AS layer
  FROM
    ethereum.core.ez_decoded_event_logs
  WHERE
    contract_address = LOWER('0x89FCb32F29e509cc42d0C8b6f058C993013A843F')
    AND lower(event_name) = 'mint'
)

SELECT
  *,
  SUM(unique_evt) OVER (
    ORDER BY day NULLS FIRST
  ) AS cumulative_evt,
  SUM(unique_L1_evt) OVER (
    ORDER BY day NULLS FIRST
  ) AS cumulative_L1_evt,
  SUM(unique_L2_evt) OVER (
    ORDER BY day NULLS FIRST
  ) AS cumulative_L2_evt
FROM (
  SELECT
    day,
    SUM(unique_evt) AS unique_evt,
    SUM(unique_L1_evt) AS unique_L1_evt,
    SUM(unique_L2_evt) AS unique_L2_evt
  FROM (
    SELECT
      DATE_TRUNC('DAY', time) AS day,
      layer,
      CASE
        WHEN unique_evt = 1 THEN 1
        ELSE 0
      END AS unique_evt,
      CASE
        WHEN unique_evt_layer = 1 AND layer = 'L1' THEN 1
        ELSE 0
      END AS unique_L1_evt,
      CASE
        WHEN unique_evt_layer = 1 AND layer = 'L2' THEN 1
        ELSE 0
      END AS unique_L2_evt
    FROM (
      SELECT
        time,
        account,
        layer,
        ROW_NUMBER() OVER (PARTITION BY account ORDER BY time) AS unique_evt,
        ROW_NUMBER() OVER (PARTITION BY account, layer ORDER BY time) AS unique_evt_layer
      FROM
        combined_mints
    )
  )
  GROUP BY day
)
ORDER BY day DESC;

--flipside query
    SELECT
      block_number,
      block_timestamp AS time,
      sum(CAST(value AS DOUBLE)) AS amount
    FROM
      ethereum.core.fact_traces
    WHERE
      to_address IN (
        LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'), -- stETH
        LOWER('0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f'), -- Withdrawal vault
        LOWER('0x388c818ca8b9251b393131c08a736a67ccb19297')  -- EL vault
      )
      AND (LOWER(type) NOT IN ('delegatecall', 'callcode', 'staticcall') OR type IS NULL)
      AND tx_status = 'SUCCESS'
      AND trace_status = 'SUCCESS'
      AND block_timestamp between cast('2021-01-13 20:30:00' AS TIMESTAMP) /* -20min */
                         and cast('2021-01-13 21:10:00' AS TIMESTAMP) /* +20min */
      group by 1,2
      order by 1,2

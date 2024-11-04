--dune query
SELECT
    block_number,
    block_time AS time,
    SUM(TRY_CAST(value AS DOUBLE)) / 1e18
  FROM ethereum.traces
  WHERE
    to IN ( 0xae7ab96520de3a18e5e111b5eaab095312d7fe84 /* stETH */, 
            0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f /* withdrawal vault */, 
            0x388c818ca8b9251b393131c08a736a67ccb19297) /* EL vault */
    AND (
      NOT LOWER(call_type) IN ('delegatecall', 'callcode', 'staticcall')
      OR call_type IS NULL
    )
    AND tx_success
    AND success
    AND block_time between cast('2021-01-13 20:30:00' AS TIMESTAMP) /* -20min */
                       and cast('2021-01-13 21:10:00' AS TIMESTAMP) /* +20min */
    GROUP BY 1,2
    order by 1,2

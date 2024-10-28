/*  from our call this morning, running on Dune: */

--dune query
SELECT *
  FROM ethereum.traces
  WHERE
    to IN (0xae7ab96520de3a18e5e111b5eaab095312d7fe84 /* stETH */, 0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f /* withdrawal vault */, 0x388c818ca8b9251b393131c08a736a67ccb19297) /* EL vault */
    AND (
      NOT LOWER(call_type) IN ('delegatecall', 'callcode', 'staticcall')
      OR call_type IS NULL
    )
    AND tx_success
    AND success
    and block_number = 11649002
    AND TRY_CAST(block_time AS DATE) = TRY_CAST('2021-01-13' AS TIMESTAMP)
    -- having SUM(TRY_CAST(value AS DOUBLE)) / 1e18 = 267.4706275001341
    order by block_number desc


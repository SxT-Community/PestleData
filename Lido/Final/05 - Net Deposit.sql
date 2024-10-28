/*     
  Potential Optimization notes: 
  - removing all unused columns (will speed up columnar stores).  
  - removed some unneeded DISTINCTs
  - some CTE steps were simple aggregations of earlier steps, in those instances
    I combined 2 or 3 CTEs into a single step
  - there was a nested CTE, I removed to flatten proceeding steps.
  - some columns were named with reserved words, like time or day.  While not 
    a problem per say, best practice is to give them a unique business name.
  - likewise, I replaced -- comments with /* */ so programatic access is easier
    (i.e., can parse out the /* */ with regex, rather than targeting EOLs)
  ------
  Further optimizations:
  - in many cases, the logic operates on timestamps rather than dates. 
    While some CTE steps do aggregate, timestamps often undo grouping / don't collapse rows.
    If possible, it would be significantly faster if we aggregated to a day-level a the earliest
    possible CTE step, then aggregated using the day-level in the final SQL.  Since the last step 
    is at a day level, it seems possible, but I'm not familiar with the business logic. 
*/
WITH 
  blocks AS (
    SELECT
      /* Lido's blocks: Select blocks mined by Lido's EL vault and calculate total gas burned. */
      block_number,
      /* block_timestamp,
      gas_used, */
      (block_header_json:base_fee_per_gas::FLOAT) * gas_used / 1e18 AS total_burn 
    FROM ethereum.core.fact_blocks
    WHERE miner = LOWER('0x388c818ca8b9251b393131c08a736a67ccb19297') /* EL vault */
  ),
 

  blocks_rewards AS (
    SELECT
    /* Aggregate transactions by block: Get total gas used and fees per block */
      t.block_number,
      MAX(t.block_timestamp) AS block_time,
      SUM(t.gas_used) AS block_gas_used,
      SUM((CAST(t.gas_used AS DOUBLE) * CAST(t.gas_price AS DOUBLE)) / 1e18) AS fee,
      fee - coalesce(b.total_burn, 0) AS block_reward 
    FROM ethereum.core.fact_transactions as t
    JOIN blocks b 
      ON t.block_number = b.block_number
    GROUP BY 1, b.total_burn
  ),


  withdrawals AS (
    /* Withdrawals from Lido's Withdrawal Vault */
    SELECT
      block_timestamp AS block_time,
      SUM(withdrawal_amount) / 1e9 AS withdrawl_amount -- Convert Gwei to ETH
    FROM ethereum.beacon_chain.ez_withdrawals
    WHERE withdrawal_address = LOWER('0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f') -- Withdrawal vault
    GROUP BY 1
  ),

  
  transfers AS (
    /* Transfers: Gather data on transfers to and from Lido addresses, including gas costs
        Optimization notes: changed from timestamp to date to reduce row count */

    SELECT
    /* Outgoing transfers from Lido addresses */
      block_timestamp as block_time,
      (-1) * SUM(CAST(value AS DOUBLE)) / 1e18 AS transfer_amount /* Convert Wei to ETH */
    FROM ethereum.core.fact_traces
    WHERE from_address IN (
        LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'), /* stETH */
        LOWER('0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f'), /* Withdrawal vault */
        LOWER('0x388c818ca8b9251b393131c08a736a67ccb19297')  /* EL vault */
      )
      AND (LOWER(type) NOT IN ('delegatecall', 'callcode', 'staticcall') OR type IS NULL)
      AND tx_status = 'SUCCESS'
      AND trace_status = 'SUCCESS'
    GROUP BY 1

    UNION ALL

    SELECT
    /* Incoming transfers to Lido addresses */
      block_timestamp as block_time,
      SUM(CAST(value AS DOUBLE)) / 1e18 AS transfer_amount /* Convert Wei to ETH */
    FROM ethereum.core.fact_traces
    WHERE to_address IN (
        LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'), /* stETH */
        LOWER('0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f'), /* Withdrawal vault */
        LOWER('0x388c818ca8b9251b393131c08a736a67ccb19297')  /* EL vault */
      )
      AND (LOWER(type) NOT IN ('delegatecall', 'callcode', 'staticcall') OR type IS NULL)
      AND tx_status = 'SUCCESS'
      AND trace_status = 'SUCCESS'
    GROUP BY 1

    UNION ALL

    SELECT
    /* Gas costs from transactions initiated by Lido addresses */
      block_timestamp as block_time,
      (-1) * SUM(CAST(gas_price AS DOUBLE) * CAST(gas_used AS DOUBLE)) / 1e18 AS transfer_amount /* Convert Wei to ETH */
    FROM ethereum.core.fact_transactions
    WHERE from_address IN (
        LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'), /* stETH */
        LOWER('0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f'), /* Withdrawal vault */
        LOWER('0x388c818ca8b9251b393131c08a736a67ccb19297')  /* EL vault */
      )
    GROUP BY 1
  ),

  lido_buffer_amounts_daily AS (
      SELECT
      /* Select the last eth_balance per day */
        DATE_TRUNC('day', block_time) AS block_date, 
        LAST_VALUE(eth_balance) OVER (
          PARTITION BY DATE_TRUNC('day', block_time)
          ORDER BY block_time
          RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS eth_balance
      FROM 
          (
          SELECT
            /* Calculate cumulative ETH balance over time */
            block_time, amount,
            SUM(amount) OVER (ORDER BY block_time
              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS eth_balance
          FROM (
              SELECT  block_time, SUM(amount) AS amount
              FROM
                (
                  SELECT block_time, transfer_amount  as amount FROM transfers   UNION ALL
                  SELECT block_time, withdrawl_amount as amount FROM withdrawals UNION ALL
                  SELECT block_time, block_reward     as amount FROM blocks_rewards 
                ) combined_data
              WHERE block_time IS NOT NULL
              GROUP BY 1
              )
          )
      QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', block_time) ORDER BY block_time DESC) = 1
    ),


  calendar AS ( 
    /* Generate a sequence of dates from November 1, 2020, to the current date */
    SELECT day
    FROM (
      SELECT DATEADD( day, SEQ4() - 1, '2020-11-01'::DATE ) AS day
      FROM TABLE(GENERATOR(ROWCOUNT => 1500)) -- Adjust ROWCOUNT as needed
    )
    WHERE day <= CURRENT_DATE()
  ),

  lido_deposits_daily AS (
    /* Calculate Lido daily deposits */
    SELECT
      DATE_TRUNC('day', block_timestamp) AS time,
      SUM(CAST(value AS DOUBLE)) / 1e18 AS lido_deposited
    FROM ethereum.core.fact_traces
    WHERE to_address = LOWER('0x00000000219ab540356cbb839cbe05303d7705fa') /* Beacon Deposit Contract */
      AND lower(type) = 'call'
      AND lower(tx_status) = 'success'
      AND lower(trace_status) = 'success'
      AND from_address IN (
        LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'), /* stETH contract */
        LOWER('0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f'), /* Lido Withdrawal Vault */
        LOWER('0xFdDf38947aFB03C621C71b06C9C70bce73f12999')  /* Lido Staking Router */
      )
    GROUP BY 1
  ),



-- Calculate Lido daily principal withdrawals
lido_principal_withdrawals_daily AS (
  SELECT
    DATE_TRUNC('day', time) AS block_time,
    (-1) * SUM(withdrawn_principal) AS amount
  FROM
      (
      SELECT
        block_timestamp AS time,
        SUM(withdrawal_amount) / 1e9 AS amount, -- Convert Gwei to ETH
        SUM(
          CASE
            WHEN withdrawal_amount / 1e9 BETWEEN 20 AND 32 THEN withdrawal_amount / 1e9
            WHEN withdrawal_amount / 1e9 > 32 THEN 32
            ELSE 0
          END
        ) AS withdrawn_principal
      FROM ethereum.beacon_chain.ez_withdrawals
      WHERE withdrawal_address = LOWER('0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f')
      GROUP BY 1
      )
  WHERE withdrawn_principal > 0
  GROUP BY 1
)

-- Final query combines all CTEs to compute daily and cumulative amounts
SELECT
  calendar.day as Calendar_Date,
  COALESCE(lido_deposits_daily.lido_deposited, 0) AS lido_deposited_daily,
  SUM(COALESCE(lido_deposits_daily.lido_deposited, 0)) OVER (
    ORDER BY calendar.day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS lido_deposited_cumu,
  COALESCE(lido_buffer_amounts_daily.eth_balance, 0) AS lido_buffer,
  COALESCE(lido_principal_withdrawals_daily.amount, 0) AS lido_withdrawals_daily,
  SUM(COALESCE(lido_principal_withdrawals_daily.amount, 0)) OVER (
    ORDER BY calendar.day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS lido_withdrawals_cumu,
  -- Calculate total ETH staked with Lido Protocol
  SUM(COALESCE(lido_deposits_daily.lido_deposited, 0)) OVER (
    ORDER BY calendar.day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) 
    + SUM(COALESCE(lido_principal_withdrawals_daily.amount, 0)) OVER (
    ORDER BY calendar.day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) 
    + COALESCE(lido_buffer_amounts_daily.eth_balance, 0) AS lido_amount
FROM
  calendar
  LEFT JOIN lido_deposits_daily ON lido_deposits_daily.time = calendar.day
  LEFT JOIN lido_buffer_amounts_daily ON lido_buffer_amounts_daily.block_date = calendar.day
  LEFT JOIN lido_principal_withdrawals_daily ON lido_principal_withdrawals_daily.block_time = calendar.day
ORDER BY
  calendar.day;
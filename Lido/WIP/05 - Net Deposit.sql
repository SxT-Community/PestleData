WITH

lido_buffer_amounts_daily AS (
  -- Begin existing query (query_2481449)
  WITH
    -- Lido's blocks: Select blocks mined by Lido's EL vault and calculate total gas burned
    blocks AS (
      SELECT
        block_number,
        block_timestamp,
        gas_used,
        (block_header_json:base_fee_per_gas::FLOAT) * gas_used / 1e18 AS total_burn
      FROM
        ethereum.core.fact_blocks
      WHERE
        miner = LOWER('0x388c818ca8b9251b393131c08a736a67ccb19297') -- EL vault
    ),

    -- Transactions in Lido's blocks: Calculate fees for each transaction
    eth_tx AS (
      SELECT
        block_timestamp,
        block_number,
        gas_used,
        (CAST(gas_used AS DOUBLE) * CAST(gas_price AS DOUBLE)) / 1e18 AS fee
      FROM
        ethereum.core.fact_transactions
      WHERE
        block_number IN (
          SELECT DISTINCT
            block_number
          FROM
            blocks
        )
    ),

    -- Aggregate transactions by block: Get total gas used and fees per block
    eth_tx_agg AS (
      SELECT
        block_number,
        MAX(block_timestamp) AS block_time,
        SUM(gas_used) AS block_gas_used,
        SUM(fee) AS fee
      FROM
        eth_tx
      GROUP BY
        block_number
    ),

    -- Block rewards: Calculate block rewards by subtracting total burn from fees
    blocks_rewards AS (
      SELECT
        t.block_number,
        t.block_time,
        t.block_gas_used,
        fee - b.total_burn AS block_reward
      FROM
        eth_tx_agg t
        LEFT JOIN blocks b ON t.block_number = b.block_number
    ),

    -- Withdrawals from Lido's Withdrawal Vault
    withdrawals AS (
      SELECT
        block_timestamp AS time,
        SUM(withdrawal_amount) / 1e9 AS amount -- Convert Gwei to ETH
      FROM
        ethereum.beacon_chain.ez_withdrawals
      WHERE
        withdrawal_address = LOWER('0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f') -- Withdrawal vault
      GROUP BY
        block_timestamp
    ),

    -- Transfers: Gather data on transfers to and from Lido addresses, including gas costs
    transfers AS (
      -- Outgoing transfers from Lido addresses
      SELECT
        block_timestamp AS time,
        (-1) * SUM(CAST(value AS DOUBLE)) / 1e18 AS amount -- Convert Wei to ETH
      FROM
        ethereum.core.fact_traces
      WHERE
        from_address IN (
          LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'), -- stETH
          LOWER('0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f'), -- Withdrawal vault
          LOWER('0x388c818ca8b9251b393131c08a736a67ccb19297')  -- EL vault
        )
        AND (LOWER(type) NOT IN ('delegatecall', 'callcode', 'staticcall') OR type IS NULL)
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'
      GROUP BY
        block_timestamp

      UNION ALL

      -- Incoming transfers to Lido addresses
      SELECT
        block_timestamp AS time,
        SUM(CAST(value AS DOUBLE)) / 1e18 AS amount -- Convert Wei to ETH
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
      GROUP BY
        block_timestamp

      UNION ALL

      -- Gas costs from transactions initiated by Lido addresses
      SELECT
        block_timestamp AS time,
        (-1) * SUM(CAST(gas_price AS DOUBLE) * CAST(gas_used AS DOUBLE)) / 1e18 AS amount -- Convert Wei to ETH
      FROM
        ethereum.core.fact_transactions
      WHERE
        from_address IN (
          LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'), -- stETH
          LOWER('0xb9d7934878b5fb9610b3fe8a5e441e8fad7e293f'), -- Withdrawal vault
          LOWER('0x388c818ca8b9251b393131c08a736a67ccb19297')  -- EL vault
        )
      GROUP BY
        block_timestamp
    ),

    -- Aggregate all financial movements by time
    aggr_data AS (
      SELECT
        time,
        SUM(amount) AS amount
      FROM
        (
          SELECT
            time,
            amount
          FROM
            transfers
          UNION ALL
          SELECT
            time,
            amount
          FROM
            withdrawals
          UNION ALL
          SELECT
            block_time AS time,
            block_reward AS amount
          FROM
            blocks_rewards
        ) combined_data
      WHERE
        time IS NOT NULL
      GROUP BY
        time
    ),

    -- Calculate cumulative ETH balance over time
    result AS (
      SELECT
        time,
        amount,
        SUM(amount) OVER (
          ORDER BY
            time
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS eth_balance
      FROM
        aggr_data
      ORDER BY
        time
    )

  -- Select the last eth_balance per day
  SELECT
    DATE_TRUNC('day', time) AS day,
    LAST_VALUE(eth_balance) OVER (
      PARTITION BY DATE_TRUNC('day', time)
      ORDER BY
        time
      RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS eth_balance
  FROM
    result
  QUALIFY ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', time) ORDER BY time DESC) = 1
),

-- Generate a sequence of dates from November 1, 2020, to the current date
calendar AS (
  SELECT
    day
  FROM (
    SELECT
      DATEADD(
        day,
        SEQ4() - 1,
        '2020-11-01'::DATE
      ) AS day
    FROM
      TABLE(GENERATOR(ROWCOUNT => 1500)) -- Adjust ROWCOUNT as needed
  )
  WHERE
    day <= CURRENT_DATE()
),

-- Calculate Lido daily deposits
lido_deposits_daily AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS time,
    SUM(CAST(value AS DOUBLE)) / 1e18 AS lido_deposited
  FROM
    ethereum.core.fact_traces
  WHERE
    to_address = LOWER('0x00000000219ab540356cbb839cbe05303d7705fa') -- Beacon Deposit Contract
    AND lower(type) = 'call'
    AND lower(tx_status) = 'success'
    AND lower(trace_status) = 'success'
    AND from_address IN (
      LOWER('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'), -- stETH contract
      LOWER('0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f'), -- Lido Withdrawal Vault
      LOWER('0xFdDf38947aFB03C621C71b06C9C70bce73f12999')  -- Lido Staking Router
    )
  GROUP BY
    1
),

-- Calculate Lido daily withdrawals
lido_all_withdrawals_daily AS (
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
  FROM
    ethereum.beacon_chain.ez_withdrawals
  WHERE
    withdrawal_address = LOWER('0xB9D7934878B5FB9610B3fE8A5e441e8fad7E293f')
  GROUP BY
    1
),

-- Calculate Lido daily principal withdrawals
lido_principal_withdrawals_daily AS (
  SELECT
    DATE_TRUNC('day', time) AS time,
    (-1) * SUM(withdrawn_principal) AS amount
  FROM
    lido_all_withdrawals_daily
  WHERE
    withdrawn_principal > 0
  GROUP BY
    1
)

-- Final query combines all CTEs to compute daily and cumulative amounts
SELECT
  calendar.day,
  COALESCE(lido_deposits_daily.lido_deposited, 0) AS lido_deposited_daily,
  SUM(COALESCE(lido_deposits_daily.lido_deposited, 0)) OVER (
    ORDER BY
      calendar.day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS lido_deposited_cumu,
  COALESCE(lido_buffer_amounts_daily.eth_balance, 0) AS lido_buffer,
  COALESCE(lido_principal_withdrawals_daily.amount, 0) AS lido_withdrawals_daily,
  SUM(COALESCE(lido_principal_withdrawals_daily.amount, 0)) OVER (
    ORDER BY
      calendar.day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS lido_withdrawals_cumu,
  -- Calculate total ETH staked with Lido Protocol
  SUM(COALESCE(lido_deposits_daily.lido_deposited, 0)) OVER (
    ORDER BY
      calendar.day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) + SUM(COALESCE(lido_principal_withdrawals_daily.amount, 0)) OVER (
    ORDER BY
      calendar.day
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) + COALESCE(lido_buffer_amounts_daily.eth_balance, 0) AS lido_amount
FROM
  calendar
  LEFT JOIN lido_deposits_daily ON lido_deposits_daily.time = calendar.day
  LEFT JOIN lido_buffer_amounts_daily ON lido_buffer_amounts_daily.day = calendar.day
  LEFT JOIN lido_principal_withdrawals_daily ON lido_principal_withdrawals_daily.time = calendar.day
ORDER BY
  calendar.day;
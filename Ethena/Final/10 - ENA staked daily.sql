WITH transfers AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    CASE
      WHEN LOWER(to_address) = LOWER('0x8bE3460A480c80728a8C4D7a5D5303c85ba7B3b9') THEN CAST(raw_amount AS DOUBLE) / 1e18  -- Stake
      WHEN LOWER(from_address) = LOWER('0x8bE3460A480c80728a8C4D7a5D5303c85ba7B3b9') THEN -1 * CAST(raw_amount AS DOUBLE) / 1e18  -- Unstake
      ELSE 0
    END AS net_amount
  FROM ethereum.core.ez_token_transfers
  WHERE
    LOWER(contract_address) = LOWER('0x57e114B691Db790C35207b2e685D4A43181e6061')  -- ENA token
    AND (
      LOWER(to_address) = LOWER('0x8bE3460A480c80728a8C4D7a5D5303c85ba7B3b9') OR
      LOWER(from_address) = LOWER('0x8bE3460A480c80728a8C4D7a5D5303c85ba7B3b9')
    )
),
daily_net AS (
  SELECT
    day,
    SUM(net_amount) AS net_stake_flow
  FROM transfers
  GROUP BY day
),
cumulative_staked AS (
  SELECT
    day,
    SUM(net_stake_flow) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS total_staked_ena
  FROM daily_net
)

SELECT *
FROM cumulative_staked
ORDER BY day desc;
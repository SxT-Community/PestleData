WITH airdrop_claims AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM(CAST(decoded_log:amount AS FLOAT)) / 1e18 AS daily_airdrop
  FROM ethereum.core.ez_decoded_event_logs
  WHERE
    tx_succeeded = 'TRUE'
    AND LOWER(contract_address) IN (
      LOWER('0x035bdaeab85e47710c27eda7fd754ba80ad4ad02'),  -- Season 1 Phase 1
      LOWER('0xf532a5a35007804a9ca79e7fa15d8f648f6d7f28'),  -- Season 1 Phase 2
      LOWER('0x2ec90ef34e312a855becf74762d198d8369eece1'),  -- Season 2
      LOWER('0xa105c3abedbaf4295ac6149bf24d5311f629934c')   -- Season 2
    )
    AND event_name = 'Claimed'
  GROUP BY 1
),

rewards_claimed AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM(CAST(decoded_log:claimedAmount AS FLOAT)) / 1e18 AS daily_rewards
  FROM ethereum.core.ez_decoded_event_logs
  WHERE 
    tx_succeeded = 'TRUE'
    AND LOWER(contract_address) = LOWER('0x7750d328b314effa365a0402ccfd489b80b0adda')  -- RewardsCoordinator
    AND event_name = 'RewardsClaimed'
    AND LOWER(decoded_log:token::STRING) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83')  -- EIGEN token
  GROUP BY 1
),

combined_daily AS (
  SELECT
    COALESCE(ac.day, rc.day) AS day,
    COALESCE(ac.daily_airdrop, 0) AS daily_airdrop,
    COALESCE(rc.daily_rewards, 0) AS daily_rewards,
    COALESCE(ac.daily_airdrop, 0) + COALESCE(rc.daily_rewards, 0) AS new_daily_circulating
  FROM airdrop_claims ac
  FULL OUTER JOIN rewards_claimed rc
    ON ac.day = rc.day
),

daily_circulating_supply AS (
  SELECT
    day,
    daily_airdrop,
    daily_rewards,
    SUM(new_daily_circulating) OVER (ORDER BY day) AS circulating_supply
  FROM combined_daily
),

token_volume AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    SUM(
      CASE WHEN LOWER(token_in) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83') THEN amount_in ELSE 0 END +
      CASE WHEN LOWER(token_out) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83') THEN amount_out ELSE 0 END
    ) AS eigen_token_volume,
    
    SUM(
      CASE WHEN LOWER(token_in) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83') THEN amount_in_usd ELSE 0 END +
      CASE WHEN LOWER(token_out) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83') THEN amount_out_usd ELSE 0 END
    ) AS eigen_token_usd_volume
  FROM ethereum.defi.ez_dex_swaps
  GROUP BY 1
  HAVING eigen_token_volume > 0
),

matched_data AS (
  SELECT 
    tv.day,
    tv.eigen_token_volume,
    tv.eigen_token_usd_volume,
    cs.circulating_supply,
    ROW_NUMBER() OVER (PARTITION BY tv.day ORDER BY cs.day DESC) AS row_num
  FROM token_volume tv
  LEFT JOIN daily_circulating_supply cs
    ON cs.day <= tv.day
)

SELECT 
  day,
  eigen_token_volume,
  eigen_token_usd_volume,
  circulating_supply AS circulating_token_supply,
  eigen_token_volume / 1e9 AS token_turnover_fully_diluted,
  eigen_token_volume / circulating_supply AS token_turnover_circulating_supply
FROM matched_data
WHERE row_num = 1
ORDER BY day DESC;

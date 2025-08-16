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
    AND LOWER(decoded_log:token::STRING) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83')  -- EIGEN
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

price_data AS (
  SELECT 
    hour,
    price,
    price * (1735442852.959785) AS fully_diluted_market_cap
  FROM ethereum.price.ez_prices_hourly
  WHERE CAST(hour AS TIME) = '00:00:00'
    AND LOWER(token_address) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83')
),

matched_data AS (
  SELECT 
    p.hour,
    p.price,
    p.fully_diluted_market_cap,
    dcs.circulating_supply,
    ROW_NUMBER() OVER (
      PARTITION BY p.hour
      ORDER BY dcs.day DESC
    ) AS row_num
  FROM price_data p
  LEFT JOIN daily_circulating_supply dcs
    ON dcs.day <= DATE(p.hour)
)

SELECT 
  hour,
  price,
  fully_diluted_market_cap,
  circulating_supply,
  price * circulating_supply AS circulating_supply_market_cap
FROM matched_data
WHERE row_num = 1
ORDER BY hour;

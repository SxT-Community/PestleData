WITH rewards_claimed AS (
  SELECT
    decoded_log:token::STRING AS token_address,
    SUM(decoded_log:claimedAmount::FLOAT) / 1e18 AS token_amount
  FROM ethereum.core.ez_decoded_event_logs
  WHERE 
    LOWER(contract_address) = LOWER('0x7750d328b314effa365a0402ccfd489b80b0adda')  
    AND event_name = 'RewardsClaimed'
    AND LOWER(decoded_log:token::STRING) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83')  
  GROUP BY token_address
),

latest_price AS (
  SELECT
    token_address,
    price
  FROM (
    SELECT
      token_address,
      price,
      ROW_NUMBER() OVER (PARTITION BY token_address ORDER BY hour DESC) AS row_num
    FROM ethereum.price.ez_prices_hourly
    WHERE LOWER(token_address) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83')
  )
  WHERE row_num = 1
)

SELECT
  r.token_address,
  r.token_amount,
  p.price AS latest_price_usd,
  r.token_amount * p.price AS incentives_usd
FROM rewards_claimed r
JOIN latest_price p ON LOWER(r.token_address) = LOWER(p.token_address)

WITH rewards_claimed AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS day,
    decoded_log:token::STRING AS token_address,
    SUM(decoded_log:claimedAmount::FLOAT) / 1e18 AS token_amount
  FROM ethereum.core.ez_decoded_event_logs
  WHERE 
    LOWER(contract_address) = LOWER('0x7750d328b314effa365a0402ccfd489b80b0adda') 
    AND event_name = 'RewardsClaimed'
    AND tx_succeeded = 'TRUE'
  GROUP BY 1, 2
),

daily_prices AS (
  SELECT
    DATE_TRUNC('day', hour) AS price_day,
    token_address,
    AVG(price) AS daily_avg_price
  FROM ethereum.price.ez_prices_hourly
  GROUP BY 1, 2
)

SELECT
  r.day,
  SUM(r.token_amount * COALESCE(p.daily_avg_price, 0)) AS daily_fees_usd
FROM rewards_claimed r
LEFT JOIN daily_prices p
  ON r.token_address = p.token_address AND r.day = p.price_day
GROUP BY r.day
ORDER BY r.day;

-- source-https://defillama.com/protocol/eigenlayer?tvl=false&fees=true&groupBy=daily
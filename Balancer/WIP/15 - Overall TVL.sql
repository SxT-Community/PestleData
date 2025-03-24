WITH balancer_pools AS (
 
  SELECT
    pool_address,
    f.value::string AS token
  FROM ethereum.defi.dim_dex_liquidity_pools,
       LATERAL FLATTEN(input => tokens) f
  WHERE platform = 'balancer'
),
daily_transfers AS (
  -- Aggregate daily token transfers (in & out) for each pool/token pair.
  SELECT
    DATE_TRUNC('day', t.block_timestamp) AS day,
    bp.pool_address,
    bp.token,
    SUM(CASE WHEN t.to_address = bp.pool_address THEN t.amount ELSE 0 END) AS tokens_in,
    SUM(CASE WHEN t.from_address = bp.pool_address THEN t.amount ELSE 0 END) AS tokens_out
  FROM ethereum.core.ez_token_transfers t
  JOIN balancer_pools bp
    ON t.CONTRACT_ADDRESS = bp.token
   AND (t.to_address = bp.pool_address OR t.from_address = bp.pool_address)
  GROUP BY 1, 2, 3
),
daily_balances AS (
  -- Compute net daily transfer and a cumulative balance for each pool/token.
  SELECT
    day,
    pool_address,
    token,
    tokens_in,
    tokens_out,
    tokens_in - tokens_out AS net_transfer,
    SUM(tokens_in - tokens_out) OVER (
      PARTITION BY pool_address, token 
      ORDER BY day 
      ROWS UNBOUNDED PRECEDING
    ) AS cumulative_balance
  FROM daily_transfers
),
price_data AS (
  -- Get the daily (midnight) price for tokens from the hourly price table.
  SELECT 
    DATE(HOUR) AS day,
    TOKEN_ADDRESS,
    PRICE
  FROM ethereum.price.ez_prices_hourly
  WHERE CAST(HOUR AS TIME) = '00:00:00'
),
USD_VALUE AS (
  -- Join the daily balances with price data to calculate USD value per token.
  SELECT
    db.day,
    db.pool_address,
    db.token,
    db.cumulative_balance,
    pd.PRICE,
    db.cumulative_balance * pd.PRICE AS usd_balance
  FROM daily_balances db
  LEFT JOIN price_data pd
    ON LOWER(db.token) = LOWER(pd.TOKEN_ADDRESS)
   AND db.day = pd.day
)
-- Now aggregate across tokens to get the total pool USD value per day.
SELECT
  day,
  pool_address,
  SUM(usd_balance) AS pool_usd_value
FROM USD_VALUE
GROUP BY day, pool_address
ORDER BY day, pool_address;
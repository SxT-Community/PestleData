WITH curve_pools AS (
  -- Extract each token from the JSON tokens column for Curve pools.
  SELECT
    pool_address,
    f.value::string AS token
  FROM ethereum.defi.dim_dex_liquidity_pools,
       LATERAL FLATTEN(input => tokens) f
  WHERE platform = 'curve'
),
daily_transfers AS (
  -- Aggregate daily token transfers (in & out) for each pool/token pair.
  SELECT
    DATE_TRUNC('day', t.block_timestamp) AS day,
    cp.pool_address,
    cp.token,
    SUM(CASE WHEN t.to_address = cp.pool_address THEN t.amount ELSE 0 END) AS tokens_in,
    SUM(CASE WHEN t.from_address = cp.pool_address THEN t.amount ELSE 0 END) AS tokens_out
  FROM ethereum.core.ez_token_transfers t
  JOIN curve_pools cp
    ON t.CONTRACT_ADDRESS = cp.token
   AND (t.to_address = cp.pool_address OR t.from_address = cp.pool_address)
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
  -- Join the daily balances with price data to calculate USD value.
  SELECT
    db.day,
    db.pool_address,
    db.token,
    db.tokens_in,
    db.tokens_out,
    db.net_transfer,
    db.cumulative_balance,
    pd.PRICE,
    db.cumulative_balance * pd.PRICE AS usd_balance
  FROM daily_balances db
  LEFT JOIN price_data pd
    ON LOWER(db.token) = LOWER(pd.TOKEN_ADDRESS)
    AND db.day = pd.day
)
SELECT *
FROM USD_VALUE
ORDER BY pool_address, token, day;
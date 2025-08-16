WITH params AS (
  SELECT 
    TO_DATE('2024-04-02') AS first_unlock_date,
    CURRENT_DATE AS end_date,  
    15000000000 AS total_supply
),
calendar_base AS (
  SELECT ROW_NUMBER() OVER (ORDER BY NULL) AS day_num
  FROM TABLE(GENERATOR(ROWCOUNT => 1500)) 
),
calendar AS (
  SELECT DATEADD(DAY, day_num - 1, (SELECT first_unlock_date FROM params)) AS day
  FROM calendar_base
  WHERE DATEADD(DAY, day_num - 1, (SELECT first_unlock_date FROM params)) <= (SELECT end_date FROM params)
),
monthly_unlocks AS (
  SELECT 
    c.day,
    CASE 
      WHEN c.day >= TO_DATE('2024-05-02') AND EXTRACT(DAY FROM c.day) = 2 THEN 0.0064 * 15000000000
      ELSE 0 
    END AS linear_2nd,
    CASE 
      WHEN c.day >= TO_DATE('2025-04-05') AND EXTRACT(DAY FROM c.day) = 5 THEN 0.0115 * 15000000000
      ELSE 0
    END AS linear_5th
  FROM calendar c
),
cliff_unlocks AS (
  SELECT 
    day,
    CASE 
      WHEN day = TO_DATE('2024-04-02') THEN 0.095 * 15000000000  
      WHEN day = TO_DATE('2024-09-02') THEN 0.0564 * 15000000000  
      WHEN day = TO_DATE('2025-03-05') THEN 0.1375 * 15000000000  
      ELSE 0 
    END AS cliff
  FROM calendar
),
combined AS (
  SELECT 
    c.day,
    COALESCE(m.linear_2nd, 0) AS linear_2nd,
    COALESCE(m.linear_5th, 0) AS linear_5th,
    COALESCE(cl.cliff, 0) AS cliff
  FROM calendar c
  LEFT JOIN monthly_unlocks m ON c.day = m.day
  LEFT JOIN cliff_unlocks cl ON c.day = cl.day
),
daily_circ_supply AS (
  SELECT
    day,
    SUM(linear_2nd + linear_5th + cliff) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS circulating_supply
  FROM combined
),
latest_price_per_day AS (
  SELECT *
  FROM (
    SELECT 
      DATE_TRUNC('day', hour) AS price_day,
      price,
      ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('day', hour) ORDER BY hour DESC) AS rn
    FROM ethereum.price.ez_prices_hourly
    WHERE symbol = 'ENA'
  ) p
  WHERE rn = 1
)

SELECT 
  d.day,
  d.circulating_supply,
  p.price,
  d.circulating_supply * p.price AS market_cap
FROM daily_circ_supply d
LEFT JOIN latest_price_per_day p
  ON d.day = p.price_day
ORDER BY d.day DESC;
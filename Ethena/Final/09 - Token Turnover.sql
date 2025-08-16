WITH token_volume AS (
  SELECT
    DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
    
    SUM(
      CASE WHEN LOWER(token_in) = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_in ELSE 0 END +
      CASE WHEN LOWER(token_out) = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_out ELSE 0 END
    ) AS ena_token_volume,
    
    SUM(
      CASE WHEN LOWER(token_in) = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_in_usd ELSE 0 END +
      CASE WHEN LOWER(token_out) = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_out_usd ELSE 0 END
    ) AS ena_token_usd_volume

  FROM ethereum.defi.ez_dex_swaps
  GROUP BY day
  HAVING SUM(
    CASE WHEN LOWER(token_in) = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_in ELSE 0 END +
    CASE WHEN LOWER(token_out) = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_out ELSE 0 END
  ) > 0
),

-- Pull in daily circulating supply from your modeled unlock query
circulating_supply_data AS (
  SELECT
    day,
    SUM(linear_2nd + linear_5th + cliff) OVER (ORDER BY day ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS circulating_supply
  FROM (
    -- Inline your existing combined unlock logic here
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
    )
    SELECT * FROM combined
  )
),

-- Match volume with latest available circulating supply
matched_data AS (
  SELECT 
    tv.day,
    tv.ena_token_volume,
    tv.ena_token_usd_volume,
    cs.circulating_supply,
    ROW_NUMBER() OVER (PARTITION BY tv.day ORDER BY cs.day DESC) AS row_num
  FROM token_volume tv
  LEFT JOIN circulating_supply_data cs
    ON cs.day <= tv.day
)

SELECT 
  day,
  ena_token_volume,
  ena_token_usd_volume,
  circulating_supply AS ena_circulating_supply,
  ena_token_volume / 15000000000 AS token_turnover_fully_diluted,
  ena_token_volume / circulating_supply AS token_turnover_circulating_supply
FROM matched_data
WHERE row_num = 1
ORDER BY day DESC;
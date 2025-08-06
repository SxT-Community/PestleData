WITH calendar_base AS (
  SELECT ROW_NUMBER() OVER (ORDER BY NULL) AS day_num
  FROM TABLE(GENERATOR(ROWCOUNT => 600))
),

calendar AS (
  SELECT DATEADD(DAY, day_num - 1, DATE('2024-03-01')) AS day
  FROM calendar_base
  WHERE DATEADD(DAY, day_num - 1, DATE('2024-03-01')) <= CURRENT_DATE
),

ena_holders AS (
  SELECT 
    user_address,
    LAST_ACTIVITY_BLOCK_TIMESTAMP::DATE AS last_active_date,
    CURRENT_BAL
  FROM ETHEREUM_ONCHAIN_CORE_DATA.CORE.EZ_CURRENT_BALANCES
  WHERE LOWER(CONTRACT_ADDRESS) = '0x57e114b691db790c35207b2e685d4a43181e6061'
    AND CURRENT_BAL > 0
),

daily_holders AS (
  SELECT
    c.day,
    COUNT(DISTINCT e.user_address) AS token_holders
  FROM calendar c
  JOIN ena_holders e
    ON e.last_active_date <= c.day
  GROUP BY c.day
)

SELECT *
FROM daily_holders
ORDER BY day DESC;
WITH daily_mint AS (
  SELECT
    DATE(block_timestamp) AS date,
    SUM(amount) AS daily_minted
  FROM
    ethereum.core.ez_token_transfers
  WHERE
    LOWER(contract_address) = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52')
    AND LOWER(from_address) = LOWER('0x0000000000000000000000000000000000000000')
  GROUP BY
    DATE(block_timestamp)
)

SELECT
  date,
  daily_minted,
  SUM(daily_minted) OVER (ORDER BY date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS circulating_supply
FROM
  daily_mint
ORDER BY
  date ASC;
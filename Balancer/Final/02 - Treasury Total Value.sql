WITH daily AS (
  SELECT 
    CAST(block_timestamp AS DATE) AS day,
    contract_address,
    SUM(CASE 
          WHEN to_address = '0x0efccbb9e2c09ea29551879bd9da32362b32fc89' THEN amount 
          ELSE 0 
        END) AS inflows,
    SUM(CASE 
          WHEN to_address = '0x0efccbb9e2c09ea29551879bd9da32362b32fc89' THEN amount_usd 
          ELSE 0 
        END) AS inflows_usd,
    SUM(CASE 
          WHEN from_address = '0x0efccbb9e2c09ea29551879bd9da32362b32fc89' THEN amount 
          ELSE 0 
        END) AS outflows,
    SUM(CASE 
          WHEN from_address = '0x0efccbb9e2c09ea29551879bd9da32362b32fc89' THEN amount_usd
          ELSE 0 
        END) AS outflows_usd
  FROM ethereum.core.ez_token_transfers
  WHERE to_address = '0x0efccbb9e2c09ea29551879bd9da32362b32fc89'
     OR from_address = '0x0efccbb9e2c09ea29551879bd9da32362b32fc89'
  GROUP BY CAST(block_timestamp AS DATE), contract_address
)
SELECT 
  day,
  contract_address,
  inflows,
  outflows,
  inflows - outflows AS net,
  SUM(inflows - outflows) OVER (
    PARTITION BY contract_address
    ORDER BY day
  ) AS cumulative_balance,
  SUM(inflows_usd - outflows_usd) OVER (
    PARTITION BY contract_address
    ORDER BY day
  ) AS cumulative_balance_usd
FROM daily
ORDER BY contract_address, day;
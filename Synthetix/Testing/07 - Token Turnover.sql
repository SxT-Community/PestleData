WITH supply_minted_data AS (
  SELECT
    CAST(decoded_log:supplyMinted AS DECIMAL(38,0)) / 1e18 AS minted,
    SUM(CAST(decoded_log:supplyMinted AS DECIMAL(38,0))) 
      OVER (ORDER BY block_timestamp) / 1e18 
      + 318950466.088773603337882496 - 164343085.67598772 AS TOTAL_SUPPLY,
    block_timestamp,
    tx_hash
  FROM
    ethereum.core.ez_decoded_event_logs
  WHERE
    (contract_address = LOWER('0x8d203C458d536Fe0F97e9f741bC231EaC8cd91cf') 
    OR contract_address = LOWER('0xA05e45396703BabAa9C276B5E5A9B6e2c175b521'))
    AND LOWER(event_name) = 'supplyminted'
  ORDER BY
    block_timestamp DESC
),
circulating_supply AS (
  SELECT
    minted / TOTAL_SUPPLY * 52 AS apr,
    minted / TOTAL_SUPPLY AS percent,
    DATE_TRUNC('day', block_timestamp) AS day,
    *
  FROM
    supply_minted_data
),
token_volume AS (
  SELECT
    DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
    SUM(CASE WHEN TOKEN_IN = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f') 
             THEN amount_in ELSE 0 END + 
        CASE WHEN TOKEN_OUT = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f') 
             THEN amount_out ELSE 0 END) AS SNX_TOKEN_VOLUME,
    
    SUM(CASE WHEN TOKEN_IN = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f') 
             THEN amount_in_usd ELSE 0 END + 
        CASE WHEN TOKEN_OUT = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f') 
             THEN amount_out_usd ELSE 0 END) AS SNX_TOKEN_USD_VOLUME
  FROM 
    ethereum.defi.ez_dex_swaps
  GROUP BY day
  HAVING SNX_TOKEN_VOLUME > 0
  ORDER BY day
),
matched_data AS (
  SELECT 
    tv.day,
    tv.SNX_TOKEN_VOLUME AS TOKEN_VOLUME,
    tv.SNX_TOKEN_USD_VOLUME AS TOKEN_USD_VOLUME,
    cs.TOTAL_SUPPLY AS CIRCULATING_TOKEN_SUPPLY,
    ROW_NUMBER() OVER (PARTITION BY tv.day ORDER BY cs.day DESC) AS row_num
  FROM 
    token_volume tv
  LEFT JOIN 
    circulating_supply cs 
  ON cs.day <= tv.day
)
SELECT 
  day,
  TOKEN_VOLUME,
  TOKEN_USD_VOLUME,
  CIRCULATING_TOKEN_SUPPLY AS TOKEN_SUPPLY,
  TOKEN_VOLUME / (CIRCULATING_TOKEN_SUPPLY) AS TOKEN_TURNOVER
FROM 
  matched_data
WHERE 
  row_num = 1
ORDER BY 
  day DESC;

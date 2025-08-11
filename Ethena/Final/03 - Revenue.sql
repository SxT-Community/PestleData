WITH token_decimals (token, decimals) AS (
  SELECT COLUMN1, COLUMN2 FROM VALUES
    ('0xc139190f447e929f090edeb554d95abb8b18ac1c', 18),  
    ('0xa2e3356610840701bdf5611a53974510ae27e2e1', 18),  
    ('0xae7ab96520de3a18e5e111b5eaab095312d7fe84', 18),  
    ('0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', 6),   
    ('0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2', 18),  
    ('0xd5f7838f5c461feff7fe49ea5ebaf7728bb0adfa', 18),  
    ('0xdac17f958d2ee523a2206206994597c13d831ec7', 6)    
),

raw_mint_fees AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS dt,
    LOWER(decoded_log:collateral_asset::STRING) AS token,
    CAST(decoded_log:collateral_amount AS DOUBLE) AS collateral_amt,
    CAST(decoded_log:usde_amount AS DOUBLE) AS usde_amt
  FROM ETHEREUM_ONCHAIN_CORE_DATA.CORE.EZ_DECODED_EVENT_LOGS
  WHERE event_name = 'Mint'
    AND LOWER(contract_address) IN (
      '0x2cc440b721d2cafd6d64908d6d8c4acc57f8afc3', -- V1
      '0xe3490297a08d6fc8da46edb7b6142e4f461b62d3'  -- V2
    )
),

mint_fees AS (
  SELECT
    m.dt,
    SUM(
      GREATEST(
        (m.collateral_amt / POWER(10, t.decimals)) - (m.usde_amt / 1e18),
        0
      )
    ) AS mint_fee
  FROM raw_mint_fees m
  LEFT JOIN token_decimals t ON m.token = t.token
  GROUP BY 1
),

reserve_inflows AS (
  SELECT
    DATE_TRUNC('day', BLOCK_TIMESTAMP) AS dt,
    SUM(AMOUNT_PRECISE) / 1e6 AS inflow  
  FROM ETHEREUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
  WHERE LOWER(TO_ADDRESS) = '0x2b5ab59163a6e93b4486f6055d33ca4a115dd4d5'  
    AND LOWER(FROM_ADDRESS) = '0x71e4f98e8f20c88112489de3dded4489802a3a87' 
    AND LOWER(CONTRACT_ADDRESS) IN (
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', -- USDC
      '0xdac17f958d2ee523a2206206994597c13d831ec7'  -- USDT
    )
  GROUP BY 1
),

all_dates AS (
  SELECT DISTINCT dt FROM mint_fees
  UNION
  SELECT DISTINCT dt FROM reserve_inflows
)

SELECT
  d.dt AS date,
  COALESCE(m.mint_fee, 0) AS mint_fees,
  COALESCE(r.inflow, 0) AS reserve_inflows,
  COALESCE(m.mint_fee, 0) + COALESCE(r.inflow, 0) AS daily_revenue
FROM all_dates d
LEFT JOIN mint_fees m ON d.dt = m.dt
LEFT JOIN reserve_inflows r ON d.dt = r.dt
ORDER BY d.dt DESC;

--source-https://defillama.com/protocol/fees/ethena
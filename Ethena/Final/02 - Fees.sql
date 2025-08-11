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
    r.dt,
    SUM(
      GREATEST(
        (r.collateral_amt / POWER(10, d.decimals)) - (r.usde_amt / 1e18),
        0
      )
    ) AS mint_fee
  FROM raw_mint_fees r
  LEFT JOIN token_decimals d ON r.token = d.token
  GROUP BY r.dt
),

reserve_inflows AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS dt,
    SUM(amount_precise) AS inflow
  FROM ETHEREUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
  WHERE LOWER(to_address) = '0x2b5ab59163a6e93b4486f6055d33ca4a115dd4d5'
    AND LOWER(from_address) = '0x71e4f98e8f20c88112489de3dded4489802a3a87'
    AND LOWER(contract_address) IN (
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
      '0xdac17f958d2ee523a2206206994597c13d831ec7'
    )
  GROUP BY 1
),

staking_outflows AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS dt,
    SUM(amount_precise) AS outflow
  FROM ETHEREUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
  WHERE LOWER(from_address) = '0xf2fa332bd83149c66b09b45670bce64746c6b439'
    AND LOWER(contract_address) IN (
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
      '0xdac17f958d2ee523a2206206994597c13d831ec7'
    )
  GROUP BY 1
),

extra_fees AS (
  SELECT
    DATE_TRUNC('day', block_timestamp) AS dt,
    SUM(amount_precise) AS extra_fee
  FROM ETHEREUM_ONCHAIN_CORE_DATA.CORE.EZ_TOKEN_TRANSFERS
  WHERE LOWER(from_address) = '0xd0ec8cc7414f27ce85f8dece6b4a58225f273311'
    AND LOWER(contract_address) IN (
      '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48',
      '0xdac17f958d2ee523a2206206994597c13d831ec7'
    )
  GROUP BY 1
),

all_dates AS (
  SELECT DISTINCT dt FROM mint_fees
  UNION
  SELECT DISTINCT dt FROM reserve_inflows
  UNION
  SELECT DISTINCT dt FROM staking_outflows
  UNION
  SELECT DISTINCT dt FROM extra_fees
)

SELECT
  d.dt AS date,
  COALESCE(m.mint_fee, 0) AS mint_fees,
  COALESCE(r.inflow, 0) AS inflows,
  COALESCE(s.outflow, 0) AS outflows,
  COALESCE(e.extra_fee, 0) AS extra_fees,
  COALESCE(m.mint_fee, 0) + COALESCE(r.inflow, 0) + COALESCE(s.outflow, 0) + COALESCE(e.extra_fee, 0) AS daily_fees
FROM all_dates d
LEFT JOIN mint_fees m ON d.dt = m.dt
LEFT JOIN reserve_inflows r ON d.dt = r.dt
LEFT JOIN staking_outflows s ON d.dt = s.dt
LEFT JOIN extra_fees e ON d.dt = e.dt
ORDER BY d.dt DESC;


--Source-https://defillama.com/protocol/fees/ethena
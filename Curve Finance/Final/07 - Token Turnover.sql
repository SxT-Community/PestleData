WITH 
  -- 1) Non-circulating addresses
  non_circ AS (
      SELECT column1 AS addr
      FROM (
          VALUES
              ('0x0000000000000000000000000000000000000000'),
              ('0x575CCD8e2D300e2377B43478339E364000318E2c'),
              ('0x629347824016530Fcd9a1990a30658ed9a04C834'),
              ('0xe3997288987E6297Ad550A69B31439504F513267'),
              ('0xf7dBC322d72C1788a1E37eEE738e2eA9C7Fa875e'),
              ('0x2A7d59E327759acd5d11A8fb652Bf4072d28AC04'),
              ('0x5f3b5DfEb7B28CDbD7FAba78963EE202a494e2A2'),
              ('0xd2D43555134dC575BF7279F4bA18809645dB0F1D'),
              ('0x827e034252937669a1484C785a5069281ee56A98'),
              ('0x81930D767a75269dC0E9b83938884E342c1Fa5F6'),
              ('0x50B5734c339B97374cf0a71B0428535EeE14B2F0'),
              ('0xD533a949740bb3306d119CC777fa900bA034cd52'),
              ('0xf22995a3EA2C83F6764c711115B23A88411CAfdd'),
              ('0x41Df5d28C7e801c4df0aB33421E2ed6ce52D2567'),
              ('0x2b6509Ca3D0FB2CD1c00F354F119aa139f118bb3'),
              ('0x679FCB9b33Fc4AE10Ff4f96caeF49c1ae3F8fA67'),
              ('0xd061D61a4d941c39E5453435B6345Dc261C2fcE0'),
              ('0x9F191D65b98C095910240F69D51cb0E4f1d33b26'),
              ('0xa445521569E93D8a87820E593bC9C51C0123da08')
      ) AS t
  ),

  -- 2) Calculate daily minted/burned CRV and cumulative
  crv_mint AS (
      SELECT
          time,
          daily_diff,
          SUM(daily_diff) OVER (ORDER BY time) AS cumm
      FROM (
          SELECT
              time,
              SUM(val) AS daily_diff
          FROM (
              -- "Mint": from non-circ => net positive supply
              SELECT
                  DATE_TRUNC('DAY', BLOCK_TIMESTAMP) AS time,
                  (AMOUNT) AS val
              FROM ethereum.core.ez_token_transfers
              WHERE lower(contract_address) = '0xd533a949740bb3306d119cc777fa900ba034cd52'
                AND lower(from_address) IN (SELECT lower(addr) FROM non_circ)

              UNION ALL

              -- "Burn": to non-circ => net negative supply
              SELECT
                  DATE_TRUNC('DAY', BLOCK_TIMESTAMP) AS time,
                  (-AMOUNT) AS val
              FROM ethereum.core.ez_token_transfers
              WHERE lower(contract_address) = '0xd533a949740bb3306d119cc777fa900ba034cd52'
                AND lower(to_address) IN (SELECT lower(addr) FROM non_circ)
          ) raw
          GROUP BY time
      )
      ORDER BY time DESC
  ),

  -- 3) Final supply CTE: rename columns to match your usage
  crv_supply AS (
      SELECT
          time AS date,         -- rename for easier joining
          cumm AS crv_supply    -- cumulative minted (minus burned) CRV
      FROM crv_mint
  ),

  -- 4) Daily token volume (unchanged from your original query)
  token_volume AS (
    SELECT
      DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
      sum(
        CASE WHEN TOKEN_IN = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52') THEN AMOUNT_IN ELSE 0 END
        + CASE WHEN TOKEN_OUT = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52') THEN AMOUNT_OUT ELSE 0 END
      ) AS TOKEN_VOLUME,
      sum(
        CASE WHEN TOKEN_IN = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52') THEN AMOUNT_IN_USD ELSE 0 END
        + CASE WHEN TOKEN_OUT = LOWER('0xD533a949740bb3306d119CC777fa900bA034cd52') THEN AMOUNT_OUT_USD ELSE 0 END
      ) AS TOKEN_USD_VOLUME
    FROM
      ethereum.defi.ez_dex_swaps
    GROUP BY
      day
    HAVING
      TOKEN_VOLUME > 0
    ORDER BY
      day
  ),

  -- 5) Match volume to supply (like your original "matched_data" CTE)
  matched_data AS (
    SELECT
      tv.day,
      tv.TOKEN_VOLUME,
      tv.TOKEN_USD_VOLUME,
      cs.crv_supply,
      ROW_NUMBER() OVER (
        PARTITION BY tv.day
        ORDER BY cs.date DESC
      ) AS row_num
    FROM token_volume tv
    LEFT JOIN crv_supply cs
           ON cs.date <= tv.day  -- match the supply on or before that day
  )

-- 6) Final SELECT: 
--    - rename crv_supply/1e18 => CIRCULATING_TOKEN_SUPPLY 
--    - keep turnover logic
SELECT
  day,
  TOKEN_VOLUME,
  TOKEN_USD_VOLUME,
  crv_supply AS CIRCULATING_TOKEN_SUPPLY,
  TOKEN_VOLUME / (2220059525285733442262148468 / 1e18) AS TOKEN_TURNOVER_FULLY_DILUTED,
  TOKEN_VOLUME / (CIRCULATING_TOKEN_SUPPLY) AS TOKEN_TURNOVER_CIRCULATING_SUPPLY
FROM matched_data
WHERE row_num = 1
ORDER BY day DESC;
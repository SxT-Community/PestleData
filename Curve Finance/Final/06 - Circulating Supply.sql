WITH 
    -- 1) Non-circulating addresses as a simple list
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
            ) AS raw
            GROUP BY time
        )
        ORDER BY time DESC
    ),
    
    -- 3) Final table to rename and produce daily + 7-day average
    final AS (
        SELECT
            time,
            cumm                AS crv_cumm,          -- total minted so far
            daily_diff          AS crv_daily_release  -- minted for that day
        FROM crv_mint
    )

SELECT
    time,
    -- rename cumm -> crv_mint
    crv_cumm AS crv_supply,

    -- 7-day average minted
    AVG(crv_daily_release) OVER (
        ORDER BY time
        ROWS BETWEEN 7 PRECEDING AND CURRENT ROW
    ) AS crv_daily_release_avg,

    -- daily minted
    crv_daily_release

FROM final
ORDER BY time DESC;
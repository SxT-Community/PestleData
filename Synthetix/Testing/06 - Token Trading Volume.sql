/*
In addition to the query below, do the same query with the SNX tokens on Optimism and on the Base chain 
by changing from ethereum.defi.ez_dex_swaps to optimism.defi.ez_dex_swaps and base.defi.ez_dex_swaps
*/

WITH ETH_TOKEN_VOL AS (
    SELECT
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(CASE 
                WHEN TOKEN_IN = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f') THEN amount_in 
                ELSE 0 
            END + 
            CASE 
                WHEN TOKEN_OUT = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f') THEN amount_out 
                ELSE 0 
            END) AS ETH_SNX_TOKEN_VOLUME,
        SUM(CASE 
                WHEN TOKEN_IN = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f') THEN amount_in_usd 
                ELSE 0 
            END + 
            CASE 
                WHEN TOKEN_OUT = LOWER('0xc011a73ee8576fb46f5e1c5751ca3b9fe0af2a6f') THEN amount_out_usd 
                ELSE 0 
            END) AS ETH_SNX_TOKEN_USD_VOLUME
    FROM 
        ethereum.defi.ez_dex_swaps
    GROUP BY day
    HAVING  ETH_SNX_TOKEN_VOLUME > 0
    ORDER BY day
),

OPT_TOKEN_VOL AS (
    SELECT
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(CASE 
                WHEN TOKEN_IN = LOWER('0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4') THEN amount_in 
                ELSE 0 
            END + 
            CASE 
                WHEN TOKEN_OUT = LOWER('0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4') THEN amount_out 
                ELSE 0 
            END) AS OPT_SNX_TOKEN_VOLUME,
        SUM(CASE 
                WHEN TOKEN_IN = LOWER('0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4') THEN amount_in_usd 
                ELSE 0 
            END + 
            CASE 
                WHEN TOKEN_OUT = LOWER('0x8700dAec35aF8Ff88c16BdF0418774CB3D7599B4') THEN amount_out_usd 
                ELSE 0 
            END) AS OPT_SNX_TOKEN_USD_VOLUME
    FROM 
        optimism.defi.ez_dex_swaps
    GROUP BY day
    HAVING  OPT_SNX_TOKEN_VOLUME > 0
    ORDER BY day
),

BASE_TOKEN_VOL AS (
    SELECT
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(CASE 
                WHEN TOKEN_IN = LOWER('0x09d51516F38980035153a554c26Df3C6f51a23C3') THEN amount_in 
                ELSE 0 
            END + 
            CASE 
                WHEN TOKEN_OUT = LOWER('0x09d51516F38980035153a554c26Df3C6f51a23C3') THEN amount_out 
                ELSE 0 
            END) AS BASE_SNX_TOKEN_VOLUME,
        SUM(CASE 
                WHEN TOKEN_IN = LOWER('0x09d51516F38980035153a554c26Df3C6f51a23C3') THEN amount_in_usd 
                ELSE 0 
            END + 
            CASE 
                WHEN TOKEN_OUT = LOWER('0x09d51516F38980035153a554c26Df3C6f51a23C3') THEN amount_out_usd 
                ELSE 0 
            END) AS BASE_SNX_TOKEN_USD_VOLUME
    FROM 
        base.defi.ez_dex_swaps
    GROUP BY day
    HAVING  BASE_SNX_TOKEN_VOLUME > 0
    ORDER BY day
)

SELECT 
    COALESCE(ETH_TOKEN_VOL.DAY, OPT_TOKEN_VOL.DAY, BASE_TOKEN_VOL.DAY) AS DAY,
    COALESCE(ETH_TOKEN_VOL.ETH_SNX_TOKEN_VOLUME, 0) AS ETH_SNX_TOKEN_VOLUME,
    COALESCE(ETH_TOKEN_VOL.ETH_SNX_TOKEN_USD_VOLUME, 0) AS ETH_SNX_TOKEN_USD_VOLUME,
    COALESCE(OPT_TOKEN_VOL.OPT_SNX_TOKEN_VOLUME, 0) AS OPT_SNX_TOKEN_VOLUME,
    COALESCE(OPT_TOKEN_VOL.OPT_SNX_TOKEN_USD_VOLUME, 0) AS OPT_SNX_TOKEN_USD_VOLUME,
    COALESCE(BASE_TOKEN_VOL.BASE_SNX_TOKEN_VOLUME, 0) AS BASE_SNX_TOKEN_VOLUME,
    COALESCE(BASE_TOKEN_VOL.BASE_SNX_TOKEN_USD_VOLUME, 0) AS BASE_SNX_TOKEN_USD_VOLUME,
    -- Total volume and USD volume
    COALESCE(ETH_TOKEN_VOL.ETH_SNX_TOKEN_VOLUME, 0) + 
    COALESCE(OPT_TOKEN_VOL.OPT_SNX_TOKEN_VOLUME, 0) + 
    COALESCE(BASE_TOKEN_VOL.BASE_SNX_TOKEN_VOLUME, 0) AS TOTAL_SNX_TOKEN_VOLUME,
    
    COALESCE(ETH_TOKEN_VOL.ETH_SNX_TOKEN_USD_VOLUME, 0) + 
    COALESCE(OPT_TOKEN_VOL.OPT_SNX_TOKEN_USD_VOLUME, 0) + 
    COALESCE(BASE_TOKEN_VOL.BASE_SNX_TOKEN_USD_VOLUME, 0) AS TOTAL_SNX_TOKEN_USD_VOLUME
FROM 
    ETH_TOKEN_VOL 
FULL OUTER JOIN 
    OPT_TOKEN_VOL ON ETH_TOKEN_VOL.DAY = OPT_TOKEN_VOL.DAY
FULL OUTER JOIN 
    BASE_TOKEN_VOL ON COALESCE(ETH_TOKEN_VOL.DAY, OPT_TOKEN_VOL.DAY) = BASE_TOKEN_VOL.DAY
ORDER BY 
    DAY;

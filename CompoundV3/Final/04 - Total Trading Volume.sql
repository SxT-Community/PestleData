-- Same as Compound V2 `Token Trading Volumne`
-- https://github.com/SxT-Community/PestleData/blob/main/CompoundV2/Final/06%20-%20Token%20Trading%20Volume.sql

SELECT
    DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
    
    sum(CASE WHEN TOKEN_IN = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN amount_in ELSE 0 END + 
        CASE WHEN TOKEN_OUT = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN amount_out ELSE 0 END) AS COMPOUND_TOKEN_VOLUME,
    
    sum(CASE WHEN TOKEN_IN = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN amount_in_usd ELSE 0 END + 
        CASE WHEN TOKEN_OUT = LOWER('0xc00e94Cb662C3520282E6f5717214004A7f26888') THEN amount_out_usd ELSE 0 END) AS COMPOUND_TOKEN_USD_VOLUME,


FROM ethereum.defi.ez_dex_swaps
GROUP BY day
HAVING COMPOUND_TOKEN_VOLUME > 0
ORDER BY day;
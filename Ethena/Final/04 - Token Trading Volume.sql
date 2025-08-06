
--note--this query needs dex_swaps table, so please refer the flipside data.

SELECT
    DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
    
    SUM(
        CASE WHEN TOKEN_IN = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_in ELSE 0 END +
        CASE WHEN TOKEN_OUT = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_out ELSE 0 END
    ) AS ethena_token_volume,
    
    SUM(
        CASE WHEN TOKEN_IN = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_in_usd ELSE 0 END +
        CASE WHEN TOKEN_OUT = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_out_usd ELSE 0 END
    ) AS ethena_token_usd_volume

FROM ethereum.defi.ez_dex_swaps

GROUP BY day
HAVING SUM(
    CASE WHEN TOKEN_IN = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_in ELSE 0 END +
    CASE WHEN TOKEN_OUT = '0x57e114b691db790c35207b2e685d4a43181e6061' THEN amount_out ELSE 0 END
) > 0

ORDER BY day;


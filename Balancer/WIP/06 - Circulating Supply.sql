-- Replicated https://dune.com/queries/4617844 on Flipside

WITH 
daily_supply AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS evt_block_time,
        SUM(CASE 
            WHEN origin_from_address = '0x0000000000000000000000000000000000000000' THEN (decoded_log:"value" / 1e18)
            ELSE -(decoded_log:"value" / 1e18) 
        END) AS daily_total_supply
    FROM
  ethereum.core.ez_decoded_event_logs
WHERE
  contract_address = LOWER('0xba100000625a3754423978a60c9317c58a424e3d')
  AND (
    origin_from_address = '0x0000000000000000000000000000000000000000'
    OR origin_to_address = '0x0000000000000000000000000000000000000000'
  )
    GROUP BY DATE_TRUNC('day', block_timestamp)
    ORDER BY evt_block_time
),
daily_locked AS (
    SELECT 
        DATE_TRUNC('day', block_timestamp) AS evt_block_time,
        tx_hash,
        origin_to_address AS address,
        CAST(decoded_log:"value" AS DECIMAL(38, 0)) AS amount
    FROM ethereum.core.ez_decoded_event_logs
    WHERE origin_to_address IN ('0x10A19e7eE7d7F8a52822f6817de8ea18204F2e4f',
                   '0xCDcEBF1f28678eb4A1478403BA7f34C94F7dDBc5',
                   '0xB129F73f1AFd3A49C701241F374dB17AE63B20Eb')
          AND contract_address = '0xba100000625a3754423978a60c9317c58a424e3d'
    
    UNION ALL
    
   SELECT 
        DATE_TRUNC('day', block_timestamp) AS evt_block_time,
        tx_hash,
        origin_from_address AS address,
        CAST(decoded_log:"value" AS DECIMAL(38, 0)) AS amount
    FROM ethereum.core.ez_decoded_event_logs
    WHERE origin_from_address IN ('0x10A19e7eE7d7F8a52822f6817de8ea18204F2e4f',
                   '0xCDcEBF1f28678eb4A1478403BA7f34C94F7dDBc5',
                   '0xB129F73f1AFd3A49C701241F374dB17AE63B20Eb')
          AND contract_address = '0xba100000625a3754423978a60c9317c58a424e3d'
    
),
net_locked AS (
    SELECT evt_block_time, SUM(amount)/1e18 AS locked_supply
    FROM daily_locked
    GROUP BY evt_block_time
    ORDER BY evt_block_time
),

Final_table AS (
    SELECT 
        COALESCE(ds.evt_block_time, nl.evt_block_time) AS evt_block_time,
        COALESCE(ds.daily_total_supply, 0) AS daily_supply,
        COALESCE(nl.locked_supply, 0) AS locked_supply
    FROM daily_supply ds
    FULL OUTER JOIN net_locked nl ON ds.evt_block_time = nl.evt_block_time
) 

SELECT evt_block_time, daily_supply, locked_supply,
       SUM(daily_supply - locked_supply) OVER (ORDER BY evt_block_time) AS accumulative_circulating,
       SUM(daily_supply) OVER (ORDER BY evt_block_time) AS accumulative_supply
FROM Final_table
ORDER BY evt_block_time;



-- -- Replicated https://dune.com/queries/4617844 on Flipside

WITH daily_balances AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        BALANCE,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('day', BLOCK_TIMESTAMP)
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS row_num
    FROM ethereum.core.fact_token_balances
    WHERE lower(USER_ADDRESS) IN ('0x10A19e7eE7d7F8a52822f6817de8ea18204F2e4f',
                   '0xCDcEBF1f28678eb4A1478403BA7f34C94F7dDBc5',
                   '0xB129F73f1AFd3A49C701241F374dB17AE63B20Eb')
      AND lower(CONTRACT_ADDRESS) = lower('0xba100000625a3754423978a60c9317c58a424e3d')
)

SELECT
    day,
    (10000000000000000000000000 - BALANCE) / 1e18 AS CIRCULATING_TOKEN_SUPPLY
FROM daily_balances
WHERE row_num = 1
ORDER BY day;
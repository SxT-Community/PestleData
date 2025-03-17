   WITH 
daily_supply AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS evt_block_time,
        SUM(CASE 
            WHEN LOWER(FROM_ADDRESS) = LOWER('0x0000000000000000000000000000000000000000') THEN (raw_amount / 1e18)
            ELSE -(raw_amount / 1e18) 
        END) AS daily_total_supply
    FROM ethereum.core.ez_token_transfers
    WHERE LOWER(CONTRACT_ADDRESS) = LOWER('0xba100000625a3754423978a60c9317c58a424e3d')
    AND (LOWER(FROM_ADDRESS) = LOWER('0x0000000000000000000000000000000000000000')
         OR LOWER(TO_ADDRESS) = LOWER('0x0000000000000000000000000000000000000000'))
    GROUP BY DATE_TRUNC('day', BLOCK_TIMESTAMP)
    ORDER BY evt_block_time
),
daily_locked AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS evt_block_time,
        tx_hash,
        LOWER(TO_ADDRESS) AS address,
        CAST(raw_amount AS DECIMAL(38, 0)) AS amount
    FROM ethereum.core.ez_token_transfers
    WHERE LOWER(TO_ADDRESS) IN (LOWER('0x10A19e7eE7d7F8a52822f6817de8ea18204F2e4f'),
                                LOWER('0xCDcEBF1f28678eb4A1478403BA7f34C94F7dDBc5'),
                                LOWER('0xB129F73f1AFd3A49C701241F374dB17AE63B20Eb'))
    AND LOWER(CONTRACT_ADDRESS) = LOWER('0xba100000625a3754423978a60c9317c58a424e3d')
    
    UNION ALL
    
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS evt_block_time,
        tx_hash,
        LOWER(FROM_ADDRESS) AS address,
        -1 * CAST(raw_amount AS DECIMAL(38, 0)) AS amount
    FROM ethereum.core.ez_token_transfers
    WHERE LOWER(FROM_ADDRESS) IN (LOWER('0x10A19e7eE7d7F8a52822f6817de8ea18204F2e4f'),
                                  LOWER('0xCDcEBF1f28678eb4A1478403BA7f34C94F7dDBc5'),
                                  LOWER('0xB129F73f1AFd3A49C701241F374dB17AE63B20Eb'))
    AND LOWER(CONTRACT_ADDRESS) = LOWER('0xba100000625a3754423978a60c9317c58a424e3d')
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
), 
circulating_supply as(
    SELECT evt_block_time AS day_, daily_supply, locked_supply,
          SUM(daily_supply - locked_supply) OVER (ORDER BY evt_block_time) AS accumulative_circulating,
          SUM(daily_supply) OVER (ORDER BY evt_block_time) AS accumulative_supply
    FROM Final_table
    ORDER BY day_
),
 

price_data AS (
    SELECT 
        HOUR,
        PRICE,
        PRICE * 67547623615456190214899638 AS MARKET_CAP
    FROM ethereum.price.ez_prices_hourly
    WHERE CAST(HOUR AS TIME) = '00:00:00' 
      AND TOKEN_ADDRESS = lower('0xba100000625a3754423978a60c9317c58a424e3d')
),
matched_data AS (
    SELECT 
        p.HOUR,
        p.PRICE,
        p.MARKET_CAP,
        cs.ACCUMULATIVE_CIRCULATING AS CIRCULATING_TOKEN_SUPPLY,
        ROW_NUMBER() OVER (PARTITION BY p.HOUR ORDER BY cs.day_ DESC) AS row_num
    FROM price_data p
    LEFT JOIN circulating_supply cs
    ON cs.day_ <= DATE(p.HOUR)
)
SELECT 
    cast(HOUR as date) as Metric_Date,
    PRICE,
    MARKET_CAP/1e18 as FULLY_DILUTED_MARKET_CAP,
    CIRCULATING_TOKEN_SUPPLY,
    PRICE * CIRCULATING_TOKEN_SUPPLY AS CIRCULATING_SUPPLY_MARKET_CAP
FROM matched_data
WHERE row_num = 1
ORDER BY 1
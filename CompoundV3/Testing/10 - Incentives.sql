WITH

-- rewards_per_day
days AS (
    SELECT DATE_TRUNC('day', block_timestamp) AS day 
    FROM ethereum.core.fact_blocks 
    GROUP BY 1
), 
update_supply_inc AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS day,
        CAST(decoded_log:newBaseTrackingSupplySpeed AS NUMBER) AS newBaseTrackingSupplySpeed,
        CAST(decoded_log:cometProxy AS STRING) AS cometProxy
    FROM ethereum.core.ez_decoded_event_logs 
    WHERE event_name='SetBaseTrackingSupplySpeed' AND contract_address = lower('0x316f9708bB98af7dA9c68C1C3b5e79039cD336E3')
    UNION ALL
    -- Append initial rate values from the contract deployment
    SELECT
        CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS day,
        CAST(newBaseTrackingSupplySpeed AS NUMBER) AS newBaseTrackingSupplySpeed,
        CAST(cometProxy AS STRING) AS cometProxy
    FROM (
        SELECT 
            402083333333 AS newBaseTrackingSupplySpeed, 
            '0xa5edbdd9646f8dff606d7448e414884c7d905dca' AS cometProxy
        UNION ALL
        SELECT 
            115740740740, 
            '0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf'
        UNION ALL
        SELECT 
            138888888888, 
            '0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07'
        UNION ALL
        SELECT 
            69444444444, 
            '0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486'
    )
),
update_borrow_inc AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS day,
        CAST(decoded_log:newBaseTrackingBorrowSpeed AS NUMBER) AS newBaseTrackingBorrowSpeed,
        CAST(decoded_log:cometProxy AS STRING) AS cometProxy
    FROM ethereum.core.ez_decoded_event_logs 
    WHERE event_name='SetBaseTrackingBorrowSpeed' AND contract_address = lower('0x316f9708bB98af7dA9c68C1C3b5e79039cD336E3')
    UNION ALL
    -- Append initial rate values from the contract deployment
    SELECT
        CAST('1970-01-01 00:00:00' AS TIMESTAMP) AS day,
        CAST(newBaseTrackingBorrowSpeed AS NUMBER) AS newBaseTrackingBorrowSpeed,
        CAST(cometProxy AS STRING) AS cometProxy
    FROM (
        SELECT 
            0 AS newBaseTrackingBorrowSpeed, 
            '0xa5edbdd9646f8dff606d7448e414884c7d905dca' AS cometProxy
        UNION ALL
        SELECT 
            0, 
            '0x9c4ec768c28520b50860ea7a15bd7213a9ff58bf'
        UNION ALL
        SELECT 
            0, 
            '0xd98Be00b5D27fc98112BdE293e487f8D4cA57d07'
        UNION ALL
        SELECT 
            0, 
            '0x6f7D514bbD4aFf3BcD1140B7344b32f063dEe486'
    )
),
comp_price AS (
    SELECT DATE_TRUNC('day', hour) AS day, AVG(price) AS avg_price
    FROM ethereum.price.ez_prices_hourly
    WHERE symbol = 'COMP' 
      AND hour >= TIMESTAMP '2023-05-04 00:00'
    GROUP BY 1
),
days_x_update_day AS (
    SELECT 
        d.day,
        usi.cometProxy,
        -- Determine the most recent `usi.day` prior to `d.day`
        LAG(usi.day, 1, NULL) OVER (PARTITION BY usi.cometProxy ORDER BY d.day) AS change_day_s,
        -- Determine the most recent `ubi.day` prior to `d.day`
        LAG(ubi.day, 1, NULL) OVER (PARTITION BY ubi.cometProxy ORDER BY d.day) AS change_day_b
    FROM days d
    LEFT JOIN update_supply_inc usi ON d.day >= usi.day
    LEFT JOIN update_borrow_inc ubi ON d.day >= ubi.day AND usi.cometProxy = ubi.cometProxy
),
rates_per_day AS (
    SELECT 
        dxu.day, 
        dxu.cometProxy, 
        usi.newBaseTrackingSupplySpeed AS trackingSupplySpeed, 
        ubi.newBaseTrackingBorrowSpeed AS trackingBorrowSpeed
    FROM days_x_update_day dxu
    LEFT JOIN update_supply_inc usi ON dxu.change_day_s = usi.day AND dxu.cometProxy = usi.cometProxy
    LEFT JOIN update_borrow_inc ubi ON dxu.change_day_b = ubi.day AND dxu.cometProxy = ubi.cometProxy
), 
rewards_per_day AS (
    SELECT 
        rpd.*, 
        (((rpd.trackingSupplySpeed + rpd.trackingBorrowSpeed) / 1e15) * 86400) AS rewards, 
        (((rpd.trackingSupplySpeed + rpd.trackingBorrowSpeed) / 1e15) * 86400 * cp.avg_price) AS rewardsUSD,
        (((rpd.trackingSupplySpeed) / 1e15) * 86400 * cp.avg_price) AS rewardsUSD_supply,
        (((rpd.trackingBorrowSpeed) / 1e15) * 86400 * cp.avg_price) AS rewardsUSD_borrow,
        cp.avg_price
    FROM rates_per_day rpd
    LEFT JOIN comp_price cp ON rpd.day = cp.day
    ORDER BY rpd.day
)
SELECT * 
FROM rewards_per_day;
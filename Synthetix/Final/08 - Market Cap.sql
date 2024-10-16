WITH supply_minted_data AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS day,
        block_timestamp,
        SUM(CAST(decoded_log:supplyMinted AS DECIMAL(38,0))) OVER (
            ORDER BY block_timestamp
        ) / 1e18 + 318950466.088773603337882496 - 164343085.67598772 AS total_supply,
        ROW_NUMBER() OVER (
            PARTITION BY DATE_TRUNC('day', block_timestamp)
            ORDER BY block_timestamp DESC
        ) AS row_num
    FROM
        ethereum.core.ez_decoded_event_logs
    WHERE
        contract_address IN (
            LOWER('0x8d203C458d536Fe0F97e9f741bC231EaC8cd91cf'),
            LOWER('0xA05e45396703BabAa9C276B5E5A9B6e2c175b521')
        )
        AND LOWER(event_name) = 'supplyminted'
),
daily_total_supply AS (
    SELECT
        day,
        total_supply
    FROM
        supply_minted_data
    WHERE
        row_num = 1
),
price_data AS (
    SELECT
        DATE_TRUNC('day', hour) AS day,
        AVG(price) AS price_usd
    FROM
        ethereum.price.ez_prices_hourly
    WHERE
        TOKEN_ADDRESS = LOWER('0xC011a73ee8576Fb46F5E1c5751cA3B9Fe0af2a6F')
        AND CAST(hour AS TIME) = '00:00:00'
    GROUP BY
        day
),
matched_data AS (
    SELECT
        p.day,
        p.price_usd,
        dts.total_supply,
        p.price_usd * dts.total_supply AS market_cap,
        ROW_NUMBER() OVER (
            PARTITION BY p.day
            ORDER BY dts.day DESC
        ) AS row_num
    FROM
        price_data p
    LEFT JOIN
        daily_total_supply dts ON dts.day <= p.day
)
SELECT
    day,
    price_usd AS price,
    total_supply,
    market_cap
FROM
    matched_data
WHERE
    row_num = 1
    AND total_supply IS NOT NULL
    AND market_cap IS NOT NULL
ORDER BY
    day;

WITH
-- Retrieve cToken and underlying token info from ez_lending_deposits
cusdc_address AS (
    SELECT DISTINCT
        LOWER(ld.protocol_market) AS token_address,          -- cToken address
        LOWER(ld.token_address) AS underlying_token_address, -- Underlying token address
        ld.token_symbol AS token_name
    FROM ethereum.defi.ez_lending_deposits AS ld
    WHERE platform = 'Compound V2'
),
-- Retrieve Mint and Redeem events (underlying amounts)
net_supplied AS (
    SELECT
        DATE(evt.block_timestamp) AS event_date,
        CASE
            WHEN evt.event_name = 'Mint' THEN LOWER(evt.decoded_log['minter']::STRING)
            WHEN evt.event_name = 'Redeem' THEN LOWER(evt.decoded_log['redeemer']::STRING)
        END AS user,
        ca.token_address,
        ca.token_name,
        SUM(
            CASE
                WHEN evt.event_name = 'Mint' THEN evt.decoded_log['mintAmount']::NUMERIC
                WHEN evt.event_name = 'Redeem' THEN -evt.decoded_log['redeemAmount']::NUMERIC
            END
        ) AS net_supplied_amount_raw
    FROM ethereum.core.ez_decoded_event_logs AS evt
    JOIN cusdc_address ca ON evt.contract_address = ca.token_address
    WHERE evt.event_name IN ('Mint', 'Redeem')
    GROUP BY event_date, user, ca.token_address, ca.token_name
),
-- Retrieve Borrow, RepayBorrow, LiquidateBorrow events (underlying amounts)
net_borrowed AS (
    SELECT
        DATE(evt.block_timestamp) AS event_date,
        LOWER(evt.decoded_log['borrower']::STRING) AS user,
        ca.token_address,
        ca.token_name,
        SUM(
            CASE
                WHEN evt.event_name = 'Borrow' THEN evt.decoded_log['borrowAmount']::NUMERIC
                WHEN evt.event_name IN ('RepayBorrow', 'LiquidateBorrow') THEN -evt.decoded_log['repayAmount']::NUMERIC
            END
        ) AS net_borrowed_amount_raw
    FROM ethereum.core.ez_decoded_event_logs AS evt
    JOIN cusdc_address ca ON evt.contract_address = ca.token_address
    WHERE evt.event_name IN ('Borrow', 'RepayBorrow', 'LiquidateBorrow')
    GROUP BY event_date, user, ca.token_address, ca.token_name
),
-- Combine daily net supplied and borrowed
positions_daily AS (
    SELECT
        event_date,
        user,
        token_address,
        token_name,
        SUM(net_supplied_amount_raw) AS net_supplied_amount_raw,
        0 AS net_borrowed_amount_raw
    FROM net_supplied
    GROUP BY event_date, user, token_address, token_name

    UNION ALL

    SELECT
        event_date,
        user,
        token_address,
        token_name,
        0 AS net_supplied_amount_raw,
        SUM(net_borrowed_amount_raw) AS net_borrowed_amount_raw
    FROM net_borrowed
    GROUP BY event_date, user, token_address, token_name
),
-- Aggregate into a single daily row per user and token
positions AS (
    SELECT
        event_date,
        user,
        token_address,
        token_name,
        SUM(net_supplied_amount_raw) AS net_supplied_amount_raw,
        SUM(net_borrowed_amount_raw) AS net_borrowed_amount_raw
    FROM positions_daily
    GROUP BY event_date, user, token_address, token_name
),
-- Transfers (optional)
transfers AS (
    SELECT
        DATE(evt.block_timestamp) AS event_date,
        LOWER(evt.decoded_log['from']::STRING) AS sender,
        LOWER(evt.decoded_log['to']::STRING) AS receiver,
        evt.decoded_log['amount']::NUMERIC AS ctoken_amount
    FROM ethereum.core.ez_decoded_event_logs AS evt
    JOIN cusdc_address ca ON evt.contract_address = ca.token_address
    WHERE evt.event_name = 'Transfer'
),
net_ctoken_balance AS (
    SELECT event_date, user, SUM(ctoken_change) AS net_ctoken_tokens
    FROM (
        SELECT event_date, sender AS user, -SUM(ctoken_amount) AS ctoken_change
        FROM transfers
        GROUP BY event_date, sender

        UNION ALL

        SELECT event_date, receiver AS user, SUM(ctoken_amount) AS ctoken_change
        FROM transfers
        GROUP BY event_date, receiver
    ) t
    GROUP BY event_date, user
),
-- Price data: exact price at midnight (no averaging)
-- Assuming one exact midnight price per underlying token.
price_data AS (
    SELECT
        DATE_TRUNC('day', hour) AS day,
        LOWER(token_address) AS token_address,
        price AS price_usd
    FROM ethereum.price.ez_prices_hourly
    WHERE CAST(hour AS TIME) = '00:00:00'
)

SELECT
    p.event_date,
    p.user,
    p.token_address AS ctoken_address,
    p.token_name,
    (SUM(p.net_supplied_amount_raw) OVER (PARTITION BY p.user, p.token_address ORDER BY p.event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 1e6) AS cumulative_supplied_usdc,
    (SUM(p.net_borrowed_amount_raw) OVER (PARTITION BY p.user, p.token_address ORDER BY p.event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) / 1e6) AS cumulative_borrowed_usdc,
    (
        (
            (
                SUM(p.net_supplied_amount_raw) OVER (PARTITION BY p.user, p.token_address ORDER BY p.event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
                - SUM(p.net_borrowed_amount_raw) OVER (PARTITION BY p.user, p.token_address ORDER BY p.event_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
            ) / 1e6
        ) * pd.price_usd
    ) AS net_balance_usd,
    pd.price_usd
FROM positions p
JOIN cusdc_address ca ON p.token_address = ca.token_address
LEFT JOIN net_ctoken_balance n
    ON p.user = n.user AND p.event_date = n.event_date
LEFT JOIN price_data pd
    ON p.event_date = pd.day
    AND ca.underlying_token_address = pd.token_address
ORDER BY p.event_date DESC, net_balance_usd DESC;
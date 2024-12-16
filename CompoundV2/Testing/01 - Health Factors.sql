WITH
-- cUSDC contract address in lowercase
cusdc_address AS (
    SELECT DISTINCT
        LOWER(ld.contract_address) AS token_address,
        ld.token_symbol AS token_name
    FROM ethereum.defi.ez_lending_deposits AS ld
    WHERE platform = 'Compound V2'
),
-- Retrieve Mint and Redeem events for cUSDC (underlying amounts)
net_supplied AS (
    SELECT
        DATE(evt.block_timestamp) AS event_date,  -- Extract date from block_timestamp
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
    JOIN cusdc_address ca
        ON evt.contract_address = ca.token_address
    WHERE evt.event_name IN ('Mint', 'Redeem')
    GROUP BY event_date, user, ca.token_address, ca.token_name
),
-- Retrieve Borrow, RepayBorrow, LiquidateBorrow events for cUSDC (underlying amounts)
net_borrowed AS (
    SELECT
        DATE(evt.block_timestamp) AS event_date,  -- Extract date from block_timestamp
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
    JOIN cusdc_address ca
        ON evt.contract_address = ca.token_address
    WHERE evt.event_name IN ('Borrow', 'RepayBorrow', 'LiquidateBorrow')
    GROUP BY event_date, user, ca.token_address, ca.token_name
),
-- Combine net supplied and net borrowed amounts per user and date
positions AS (
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
-- Incorporate Transfer events for cUSDC tokens
-- Note: This tracks cUSDC token flows, not underlying amounts.
transfers AS (
    SELECT
        DATE(evt.block_timestamp) AS event_date,  -- Extract date from block_timestamp
        LOWER(evt.decoded_log['from']::STRING) AS sender,
        LOWER(evt.decoded_log['to']::STRING) AS receiver,
        evt.decoded_log['amount']::NUMERIC AS ctoken_amount
    FROM ethereum.core.ez_decoded_event_logs AS evt
    JOIN cusdc_address ca
        ON evt.contract_address = ca.token_address
    WHERE evt.event_name = 'Transfer'
),
-- Aggregate net cUSDC token flows per user and date
net_ctoken_balance AS (
    SELECT event_date, user, SUM(ctoken_change) AS net_ctoken_tokens
    FROM (
        -- Tokens leaving the sender
        SELECT event_date, sender AS user, -SUM(ctoken_amount) AS ctoken_change
        FROM transfers
        GROUP BY event_date, sender
        UNION ALL
        -- Tokens entering the receiver
        SELECT event_date, receiver AS user, SUM(ctoken_amount) AS ctoken_change
        FROM transfers
        GROUP BY event_date, receiver
    ) t
    GROUP BY event_date, user
),
-- Price data for USDC
price_data AS (
    SELECT
        DATE_TRUNC('day', hour) AS day,
        AVG(price) AS price_usd
    FROM ethereum.price.ez_prices_hourly
    WHERE CAST(hour AS TIME) = '00:00:00'
    GROUP BY day
)

SELECT
    p.event_date,
    p.user,
    -- Convert underlying amounts to USDC units
    (SUM(p.net_supplied_amount_raw) / 1e6) AS net_supplied_usdc,
    (SUM(p.net_borrowed_amount_raw) / 1e6) AS net_borrowed_usdc,
    ((SUM(p.net_supplied_amount_raw) - SUM(p.net_borrowed_amount_raw)) / 1e6) * pd.price_usd AS net_balance_usd,
    p.token_address,
    p.token_name
FROM positions p
LEFT JOIN net_ctoken_balance n
    ON p.user = n.user AND p.event_date = n.event_date
LEFT JOIN price_data pd
    ON p.event_date = pd.day
GROUP BY p.event_date, p.user, p.token_address, p.token_name, pd.price_usd
ORDER BY p.event_date DESC, net_balance_usd DESC;

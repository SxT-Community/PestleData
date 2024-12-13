WITH
-- cUSDC contract address 
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
    GROUP BY user, ca.token_address, ca.token_name
),
-- Retrieve Borrow, RepayBorrow, LiquidateBorrow events for cUSDC (underlying amounts)
net_borrowed AS (
    SELECT
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
    GROUP BY user, ca.token_address, ca.token_name
),
-- Combine net supplied and net borrowed amounts per user
positions AS (
    SELECT
        user,
        token_address,
        token_name,
        SUM(net_supplied_amount_raw) AS net_supplied_amount_raw,
        0 AS net_borrowed_amount_raw
    FROM net_supplied
    GROUP BY user, token_address, token_name
    UNION ALL
    SELECT
        user,
        token_address,
        token_name,
        0 AS net_supplied_amount_raw,
        SUM(net_borrowed_amount_raw) AS net_borrowed_amount_raw
    FROM net_borrowed
    GROUP BY user, token_address, token_name
),
-- Incorporate Transfer events for cUSDC tokens
-- Note: This tracks cUSDC token flows, not underlying amounts.
transfers AS (
    SELECT
        LOWER(evt.decoded_log['from']::STRING) AS sender,
        LOWER(evt.decoded_log['to']::STRING) AS receiver,
        evt.decoded_log['amount']::NUMERIC AS ctoken_amount
    FROM ethereum.core.ez_decoded_event_logs AS evt
    JOIN cusdc_address ca
        ON evt.contract_address = ca.token_address
    WHERE evt.event_name = 'Transfer'
),
-- Aggregate net cUSDC token flows per user
net_ctoken_balance AS (
    SELECT user, SUM(ctoken_change) AS net_ctoken_tokens
    FROM (
        -- Tokens leaving the sender
        SELECT sender AS user, -SUM(ctoken_amount) AS ctoken_change
        FROM transfers
        GROUP BY sender
        UNION ALL
        -- Tokens entering the receiver
        SELECT receiver AS user, SUM(ctoken_amount) AS ctoken_change
        FROM transfers
        GROUP BY receiver
    ) t
    GROUP BY user
)

SELECT
    p.user,
    -- Convert underlying amounts to USDC units
    (SUM(p.net_supplied_amount_raw) / 1e6) AS net_supplied_usdc,
    (SUM(p.net_borrowed_amount_raw) / 1e6) AS net_borrowed_usdc,
    ((SUM(p.net_supplied_amount_raw) - SUM(p.net_borrowed_amount_raw)) / 1e6) AS net_balance_usdc,
    p.token_address,
    p.token_name
FROM positions p
LEFT JOIN net_ctoken_balance n
    ON p.user = n.user
GROUP BY p.user, p.token_address, p.token_name
ORDER BY net_balance_usdc DESC;
WITH
-- cUSDC contract address in lowercase
cusdc_address AS (
    SELECT '0x39aa39c021dfbae8fac545936693ac917d5e7563' AS ctoken_address
),
-- Retrieve Mint and Redeem events for cUSDC (underlying amounts)
net_supplied AS (
    SELECT
        CASE
            WHEN evt.event_name = 'Mint' THEN LOWER(evt.decoded_log['minter']::STRING)
            WHEN evt.event_name = 'Redeem' THEN LOWER(evt.decoded_log['redeemer']::STRING)
        END AS user,
        SUM(
            CASE
                WHEN evt.event_name = 'Mint' THEN evt.decoded_log['mintAmount']::NUMERIC
                WHEN evt.event_name = 'Redeem' THEN -evt.decoded_log['redeemAmount']::NUMERIC
            END
        ) AS net_supplied_amount_raw
    FROM ethereum.core.ez_decoded_event_logs AS evt
    CROSS JOIN cusdc_address
    WHERE LOWER(evt.contract_address) = cusdc_address.ctoken_address
      AND evt.event_name IN ('Mint', 'Redeem')
    GROUP BY user
),
-- Retrieve Borrow, RepayBorrow, LiquidateBorrow events for cUSDC (underlying amounts)
net_borrowed AS (
    SELECT
        LOWER(evt.decoded_log['borrower']::STRING) AS user,
        SUM(
            CASE
                WHEN evt.event_name = 'Borrow' THEN evt.decoded_log['borrowAmount']::NUMERIC
                WHEN evt.event_name IN ('RepayBorrow', 'LiquidateBorrow') THEN -evt.decoded_log['repayAmount']::NUMERIC
            END
        ) AS net_borrowed_amount_raw
    FROM ethereum.core.ez_decoded_event_logs AS evt
    CROSS JOIN cusdc_address
    WHERE LOWER(evt.contract_address) = cusdc_address.ctoken_address
      AND evt.event_name IN ('Borrow', 'RepayBorrow', 'LiquidateBorrow')
    GROUP BY user
),
-- Combine net supplied and net borrowed amounts per user
positions AS (
    SELECT
        COALESCE(ns.user, nb.user) AS user,
        COALESCE(ns.net_supplied_amount_raw, 0) AS net_supplied_amount_raw,
        COALESCE(nb.net_borrowed_amount_raw, 0) AS net_borrowed_amount_raw
    FROM net_supplied ns
    FULL OUTER JOIN net_borrowed nb ON ns.user = nb.user
),

-- Incorporate Transfer events for cUSDC tokens
-- Note: This tracks cUSDC token flows, not underlying amounts.
transfers AS (
    SELECT
        LOWER(evt.decoded_log['from']::STRING) AS sender,
        LOWER(evt.decoded_log['to']::STRING) AS receiver,
        evt.decoded_log['amount']::NUMERIC AS ctoken_amount
    FROM ethereum.core.ez_decoded_event_logs AS evt
    CROSS JOIN cusdc_address
    WHERE LOWER(evt.contract_address) = cusdc_address.ctoken_address
      AND evt.event_name = 'Transfer'
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
    (p.net_supplied_amount_raw / 1e6) AS net_supplied_usdc,
    (p.net_borrowed_amount_raw / 1e6) AS net_borrowed_usdc,
    ((p.net_supplied_amount_raw - p.net_borrowed_amount_raw) / 1e6) AS net_balance_usdc,
    n.net_ctoken_tokens AS net_ctokens
FROM positions p
LEFT JOIN net_ctoken_balance n ON p.user = n.user
ORDER BY net_balance_usdc DESC;
WITH comptrollers AS (
    SELECT DISTINCT decoded_log:"controller"::STRING AS controller
    FROM ethereum.core.ez_decoded_event_logs
    WHERE 
        lower(origin_from_address) = lower('0xbabe61887f1de2713c6f97e567623453d3C79f67') 
        AND lower(event_name) = lower('NewVault')
),
token_transfers AS (
    SELECT
        t.block_timestamp::DATE AS transfer_date,
        t.to_address AS receiver,
        t.from_address AS sender,
        t.amount::FLOAT AS amount,
        t.amount_usd::FLOAT AS amount_usd,
        t.contract_address,
        t.symbol,
        c.controller
    FROM ethereum.core.ez_token_transfers t
    JOIN comptrollers c 
        ON lower(t.to_address) = lower(c.controller) 
        OR lower(t.from_address) = lower(c.controller)
)
SELECT
    transfer_date,
    controller,
    contract_address,
    symbol,
    SUM(CASE WHEN receiver = controller THEN amount ELSE 0 END) AS inflows,
    SUM(CASE WHEN sender = controller THEN amount ELSE 0 END) AS outflows,
    SUM(SUM(CASE WHEN receiver = controller THEN amount ELSE -amount END)) 
        OVER (PARTITION BY controller, contract_address ORDER BY transfer_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS cumulative_balance,
    SUM(SUM(CASE WHEN receiver = controller THEN amount_usd ELSE -amount_usd END)) 
        OVER (PARTITION BY controller, contract_address ORDER BY transfer_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) 
        AS cumulative_balance_usd
FROM token_transfers
GROUP BY transfer_date, controller, contract_address, symbol
ORDER BY transfer_date, controller, contract_address;
WITH comptrollers AS (
    SELECT DISTINCT decoded_log:"controller"::STRING AS controller
    FROM ethereum.core.ez_decoded_event_logs
    WHERE 
        lower(contract_address) = lower('0xeA6876DDE9e3467564acBeE1Ed5bac88783205E0') 
        AND lower(event_name) = lower('NewVault')
),
token_transfers AS (
    SELECT
        t.block_timestamp::DATE AS transfer_date,
        t.to_address AS receiver,
        t.from_address AS sender,
        t.amount_usd::FLOAT AS amount_usd,
        c.controller
    FROM ethereum.core.ez_token_transfers t
    JOIN comptrollers c 
        ON lower(t.to_address) = lower(c.controller) 
        OR lower(t.from_address) = lower(c.controller)
),
daily_usd_balance AS (
    SELECT
        transfer_date,
        SUM(CASE WHEN receiver = controller THEN amount_usd ELSE -amount_usd END) AS net_usd_change
    FROM token_transfers
    GROUP BY transfer_date
)
SELECT 
    transfer_date,
    SUM(net_usd_change) OVER (
        ORDER BY transfer_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_balance_usd
FROM daily_usd_balance
ORDER BY transfer_date
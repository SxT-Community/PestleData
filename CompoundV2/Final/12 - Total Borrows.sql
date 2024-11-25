WITH filtered_events AS (
    SELECT
        DATE(block_timestamp) AS event_date,
        LOWER(logs.contract_address) AS contract_address,
        decoded_log:"totalBorrows"::NUMBER AS total_borrows,
        ROW_NUMBER() OVER (
            PARTITION BY DATE(block_timestamp), LOWER(logs.contract_address)
            ORDER BY block_timestamp DESC
        ) AS row_num,
        deposits.token_name
    FROM
        Ethereum.core.fact_decoded_event_logs AS logs
    INNER JOIN (
        SELECT DISTINCT 
            LOWER(contract_address) AS contract_address,
            token_symbol AS token_name
        FROM 
            ethereum.defi.ez_lending_deposits
        WHERE 
            platform = 'Compound V2'
    ) AS deposits
    ON LOWER(logs.contract_address) = deposits.contract_address
    WHERE logs.event_name = 'Borrow'
)
SELECT
    event_date,
    contract_address,
    token_name,
    total_borrows
FROM
    filtered_events
WHERE
    row_num = 1
order by event_date
    
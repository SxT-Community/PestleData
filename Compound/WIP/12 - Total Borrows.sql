WITH filtered_events AS (
    SELECT
        DATE(block_timestamp) AS event_date,
        LOWER(contract_address) AS contract_address,
        decoded_log:"totalBorrows"::NUMBER AS total_borrows,
        ROW_NUMBER() OVER (
            PARTITION BY DATE(block_timestamp), LOWER(contract_address)
            ORDER BY block_timestamp DESC
        ) AS row_num
    FROM
        ethereum.core.fact_decoded_event_logs
    WHERE
        LOWER(contract_address) IN (
            '0x39aa39c021dfbae8fac545936693ac917d5e7563',  -- cUSDC
            '0xccf4429db6322d5c611ee964527d42e5d685dd6a',  -- cWBTC
            '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5',  -- cETH
            '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9'   -- cUSDT
        )
        AND event_name = 'Borrow'
),
mapped_events AS (
    SELECT
        event_date,
        contract_address,
        total_borrows,
        CASE LOWER(contract_address)
            WHEN '0x39aa39c021dfbae8fac545936693ac917d5e7563' THEN 'cUSDC'
            WHEN '0xccf4429db6322d5c611ee964527d42e5d685dd6a' THEN 'cWBTC'
            WHEN '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5' THEN 'cETH'
            WHEN '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9' THEN 'cUSDT'
            ELSE 'Unknown'
        END AS token_name
    FROM
        filtered_events
    WHERE
        row_num = 1
)
SELECT
    event_date,
    token_name,
    CASE token_name
        WHEN 'cUSDC' THEN '0x39aa39c021dfbae8fac545936693ac917d5e7563'
        WHEN 'cWBTC' THEN '0xccf4429db6322d5c611ee964527d42e5d685dd6a'
        WHEN 'cETH'  THEN '0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5'
        WHEN 'cUSDT' THEN '0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9'
        ELSE '0x0000000000000000000000000000000000000000' -- Default or Unknown Address
    END AS token_address,
    total_borrows AS TOTAL_BORROWS
FROM
    mapped_events
ORDER BY
    event_date,
    token_name;
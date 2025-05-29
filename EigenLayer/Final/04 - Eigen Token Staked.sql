WITH Flow_Transactions AS (
    SELECT
        DATE_TRUNC('day', block_timestamp) AS date,
        raw_amount / -1e18 AS value
    FROM ethereum.core.ez_token_transfers
    WHERE
        LOWER(contract_address) = LOWER('0x83E9115d334D248Ce39a6f36144aEaB5b3456e75')
        AND LOWER(from_address) = LOWER('0xaCB55C530Acdb2849e6d4f36992Cd8c9D50ED8F7')
    
    UNION ALL
    
    SELECT
        DATE_TRUNC('day', block_timestamp) AS date,
        raw_amount / 1e18 AS value
    FROM ethereum.core.ez_token_transfers
    WHERE
        LOWER(contract_address) = LOWER('0x83E9115d334D248Ce39a6f36144aEaB5b3456e75')
        AND LOWER(to_address) = LOWER('0xaCB55C530Acdb2849e6d4f36992Cd8c9D50ED8F7')
)

SELECT
    date,
    SUM(value) AS daily_flow,
    COALESCE(
        SUM(SUM(value)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
        0
    ) AS circulating_beigen_supply
FROM
    Flow_Transactions
GROUP BY
    date
ORDER BY
    date DESC;


    -- Dune reference query-https://dune.com/queries/4121028/6939223
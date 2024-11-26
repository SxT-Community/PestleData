WITH 
-- Step 1: Get the relevant cToken addresses, token names, and underlying token addresses
lending_info AS (
    SELECT DISTINCT 
        LOWER(deposits.contract_address) AS ctoken_contract_address,
        LOWER(deposits.token_address) AS underlying_token_address,
        deposits.token_symbol AS token_name
    FROM 
        ethereum.defi.ez_lending_deposits AS deposits
    WHERE 
        deposits.platform = 'Compound V2'
),

-- Query 1: Total interest accumulated by day and contract
interest_accumulated_cte AS (
    SELECT
        DATE(logs.block_timestamp) AS calculation_date,
        LOWER(logs.contract_address) AS ctoken_contract_address,
        lending_info.token_name,
        SUM(
            TRY_TO_DECIMAL(logs.decoded_log:"interestAccumulated"::STRING) / POW(10, 6) -- Adjust decimals if needed
        ) AS daily_interest_accumulated
    FROM
        ethereum.core.fact_decoded_event_logs AS logs
    INNER JOIN lending_info
        ON LOWER(logs.contract_address) = lending_info.ctoken_contract_address
    WHERE
        LOWER(logs.event_name) = 'accrueinterest'
    GROUP BY
        DATE(logs.block_timestamp), LOWER(logs.contract_address), lending_info.token_name
),

-- Query 2: Net amount by day and contract
net_amount_cte AS (
    SELECT
        DATE(transfers.block_timestamp) AS calculation_date,
        ctoken.ctoken_contract_address AS contract_address,
        ctoken.token_name,
        SUM(CASE WHEN LOWER(transfers.to_address) = ctoken.ctoken_contract_address THEN transfers.amount ELSE 0 END) -
        SUM(CASE WHEN LOWER(transfers.from_address) = ctoken.ctoken_contract_address THEN transfers.amount ELSE 0 END) AS daily_net_amount
    FROM
        ethereum.core.ez_token_transfers AS transfers
    INNER JOIN lending_info AS ctoken
        ON LOWER(transfers.contract_address) = ctoken.underlying_token_address
    WHERE
        LOWER(transfers.to_address) = ctoken.ctoken_contract_address
        OR LOWER(transfers.from_address) = ctoken.ctoken_contract_address
    GROUP BY
        DATE(transfers.block_timestamp), ctoken.ctoken_contract_address, ctoken.token_name
),

-- Query 3: Net supplied by day and contract
net_supplied_cte AS (
    SELECT
        DATE(logs.block_timestamp) AS calculation_date,
        LOWER(logs.contract_address) AS ctoken_contract_address,
        lending_info.token_name,
        SUM(
            CASE WHEN LOWER(logs.event_name) = 'borrow' THEN TRY_TO_DECIMAL(logs.decoded_log:"borrowAmount"::STRING) / POW(10, 6) ELSE 0 END
        ) -
        SUM(
            CASE WHEN LOWER(logs.event_name) = 'repayborrow' THEN TRY_TO_DECIMAL(logs.decoded_log:"repayAmount"::STRING) / POW(10, 6) ELSE 0 END
        ) AS daily_net_supplied
    FROM
        ethereum.core.fact_decoded_event_logs AS logs
    INNER JOIN lending_info
        ON LOWER(logs.contract_address) = lending_info.ctoken_contract_address
    WHERE
        LOWER(logs.event_name) IN ('borrow', 'repayborrow')
    GROUP BY
        DATE(logs.block_timestamp), LOWER(logs.contract_address), lending_info.token_name
)

-- Combine results and calculate running totals by day and contract
SELECT
    d.calculation_date,
    d.ctoken_contract_address AS contract_address,
    d.token_name,
    -- Running total for each metric partitioned by contract_address and token_name
    SUM(COALESCE(i.daily_interest_accumulated, 0)) OVER (
        PARTITION BY d.ctoken_contract_address, d.token_name ORDER BY d.calculation_date
    ) AS total_interest_accumulated,
    SUM(COALESCE(n.daily_net_amount, 0)) OVER (
        PARTITION BY d.ctoken_contract_address, d.token_name ORDER BY d.calculation_date
    ) AS net_amount,
    SUM(COALESCE(s.daily_net_supplied, 0)) OVER (
        PARTITION BY d.ctoken_contract_address, d.token_name ORDER BY d.calculation_date
    ) AS net_supplied,
    -- Running total sum
    SUM(
        COALESCE(i.daily_interest_accumulated, 0) +
        COALESCE(n.daily_net_amount, 0) +
        COALESCE(s.daily_net_supplied, 0)
    ) OVER (
        PARTITION BY d.ctoken_contract_address, d.token_name ORDER BY d.calculation_date
    ) AS total_sum
FROM
    -- Create a calendar of all calculation dates for each contract
    (
        SELECT DISTINCT 
            DATE(block_timestamp) AS calculation_date, 
            LOWER(contract_address) AS ctoken_contract_address,
            token_name
        FROM
            (
                SELECT logs.block_timestamp, logs.contract_address, lending_info.token_name
                FROM ethereum.core.fact_decoded_event_logs AS logs
                INNER JOIN lending_info
                    ON LOWER(logs.contract_address) = lending_info.ctoken_contract_address
                UNION
                SELECT transfers.block_timestamp, ctoken.ctoken_contract_address AS contract_address, ctoken.token_name
                FROM ethereum.core.ez_token_transfers AS transfers
                INNER JOIN lending_info AS ctoken
                    ON LOWER(transfers.contract_address) = ctoken.underlying_token_address
                WHERE
                    LOWER(transfers.to_address) = ctoken.ctoken_contract_address
                    OR LOWER(transfers.from_address) = ctoken.ctoken_contract_address
            ) AS combined_data
    ) d
LEFT JOIN
    interest_accumulated_cte i
    ON d.calculation_date = i.calculation_date 
    AND d.ctoken_contract_address = i.ctoken_contract_address 
    AND d.token_name = i.token_name
LEFT JOIN
    net_amount_cte n
    ON d.calculation_date = n.calculation_date 
    AND d.ctoken_contract_address = n.contract_address 
    AND d.token_name = n.token_name
LEFT JOIN
    net_supplied_cte s
    ON d.calculation_date = s.calculation_date 
    AND d.ctoken_contract_address = s.ctoken_contract_address 
    AND d.token_name = s.token_name
WHERE
    -- Filter out rows where all metrics are zero
    COALESCE(i.daily_interest_accumulated, 0) != 0
    OR COALESCE(n.daily_net_amount, 0) != 0
    OR COALESCE(s.daily_net_supplied, 0) != 0
ORDER BY
    d.calculation_date, d.ctoken_contract_address, d.token_name;
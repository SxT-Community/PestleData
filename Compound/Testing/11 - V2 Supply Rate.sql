WITH interest_accumulated AS (
    SELECT
        DATE_TRUNC('day', a.block_timestamp) AS date,
        a.contract_address,
        SUM(TRY_TO_NUMBER(a.decoded_log:"interestAccumulated"::STRING)) AS interest,
        AVG(TRY_TO_NUMBER(a.decoded_log:"totalBorrows"::STRING)) AS borrowed
    FROM
        ethereum.core.ez_decoded_event_logs AS a
    WHERE
        lower(a.event_name) = 'accrueinterest'
        AND lower(a.contract_address) IN (
            lower('0x39aa39c021dfbae8fac545936693ac917d5e7563'),  -- cUSDC
            lower('0xccF4429DB6322D5C611ee964527D42E5d685DD6a'),  -- cWBTC
            lower('0x4Ddc2D193948926D02f9B1fE9e1daa0718270ED5'),  -- cETH
            lower('0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9')   -- cUSDT
        )
    GROUP BY
        date,
        a.contract_address
),
adjusted_values AS (
    SELECT
        date,
        CASE
            WHEN lower(contract_address) = lower('0x39aa39c021dfbae8fac545936693ac917d5e7563') THEN 'cUSDC'
            WHEN lower(contract_address) = lower('0xf650c3d88d12db855b8bf7d11be6c55a4e07dcc9') THEN 'cUSDT'
            WHEN lower(contract_address) = lower('0xccf4429db6322d5c611ee964527d42e5d685dd6a') THEN 'cWBTC'
            WHEN lower(contract_address) = lower('0x4ddc2d193948926d02f9b1fe9e1daa0718270ed5') THEN 'cETH'
            ELSE 'Unknown'
        END AS symbol,
        8 AS decimals,  -- Assuming decimals is 8 for these tokens
        interest,
        borrowed
    FROM
        interest_accumulated
),
avg_rates AS (
    SELECT
        date,
        symbol,
        apy AS "1d_ta",
        AVG(apy) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS "7d_ta",
        AVG(apy) OVER (
            PARTITION BY symbol
            ORDER BY date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS "30d_ta"
    FROM (
        SELECT
            date,
            symbol,
            100.0 * (
                POWER(1 + ((interest / POWER(10, decimals)) / (borrowed / POWER(10, decimals))), 365) - 1
            ) AS apy
        FROM
            adjusted_values
    ) AS sub
    WHERE
        date < DATE_TRUNC('day', CURRENT_TIMESTAMP)
)
SELECT
    *
FROM
    avg_rates
WHERE
    date > DATEADD(day, -1095, DATE_TRUNC('day', CURRENT_TIMESTAMP))  -- 3 years
ORDER BY
    date DESC;

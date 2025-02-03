-- # WIP Incentives Query

WITH emissions AS (
    SELECT
        day
        ,CASE WHEN day > '2020-08-13' AND day <= '2021-08-13'
                THEN 'Year 1'
            WHEN day > '2021-08-13' AND day <= '2022-08-13'
                THEN 'Year 2'
            WHEN day > '2022-08-13' AND day <= '2023-08-13'
                THEN 'Year 3'
            WHEN day > '2023-08-13' AND day <= '2024-08-13'
                THEN 'Year 4'
            WHEN day > '2024-08-13' AND day <= '2025-08-13'
                THEN 'Year 5'
            ELSE 'update code'
            END AS emission_year
        ,crv_price
        ,liq_incentives
        ,liq_incentives * crv_price AS emissions_usd
    FROM (
        SELECT
            day
            ,AVG(crv_price) as crv_price
        FROM (
            SELECT
                DATE_TRUNC('day',"minute") AS day
                ,"price" AS crv_price
            FROM prices.usd
            WHERE contract_address = '\xD533a949740bb3306d119CC777fa900bA034cd52' -- CRV token
            ORDER BY 1 DESC
            ) sub1
        GROUP BY 1
        ) sub2
    LEFT JOIN dune_user_generated."blockworks_crv_supply_schedule" b ON sub2.day = b.time
    GROUP BY 1,2,3,4
    ORDER BY 1 DESC
),

ey AS (
SELECT
    distinct emission_year
    ,SUM(emissions_usd) OVER(PARTITION BY emission_year) ey_usd
FROM emissions
)

SELECT 
    emission_year
    ,CASE WHEN emission_year <> 'Year 3'
        THEN ey_usd
    END AS usd
    ,CASE WHEN emission_year = 'Year 3'
        THEN ey_usd
    END AS usd
FROM ey
-- Replicated query from dune https://dune.com/queries/1175558

WITH date_series AS (
    SELECT DATEADD(DAY, ROW_NUMBER() OVER(ORDER BY NULL) - 1, '2020-08-14') AS time
    FROM TABLE(GENERATOR(ROWCOUNT => 3653)) -- Generates ~10 years of daily dates
)

, blockworks_crv_supply_schedule AS (
    SELECT
        time,
        CASE WHEN time <= '2021-08-13' THEN 752918.583561644 
             WHEN time <= '2022-08-13' THEN 633126.537894891 
             WHEN time <= '2023-08-13' THEN 532393.836117809 
             WHEN time <= '2024-08-13' THEN 446464.876851211 
             WHEN time <= '2025-08-13' THEN 376459.291780822 
             WHEN time <= '2026-08-13' THEN 316563.268947446 
             WHEN time <= '2027-08-13' THEN 266196.918058904 
             WHEN time <= '2028-08-13' THEN 223232.438425606 
             WHEN time <= '2029-08-13' THEN 188229.645890411 
             WHEN time <= '2030-08-13' THEN 158281.634473723 
             ELSE 133098.459029452 
        END AS liq_incentives,
        CASE WHEN time <= '2021-08-13' THEN 415110.00436772 ELSE 0 END AS early_users,
        CASE WHEN time <= '2021-08-13' THEN 548603.529712329 
             WHEN time <= '2022-08-13' THEN 548603.529712329 
             WHEN time <= '2023-08-13' THEN 548603.529712329 
             WHEN time <= '2024-08-13' THEN 547104.612964481 
             ELSE 0 
        END AS core_team,
        CASE WHEN time <= '2021-08-13' THEN 148122.953027397 
             WHEN time <= '2022-08-13' THEN 148122.953027397 
             ELSE 0 
        END AS investors,
        CASE WHEN time <= '2021-08-13' THEN 124533.001310316 
             WHEN time <= '2022-08-13' THEN 124533.001310316 
             ELSE 0 
        END AS employees,
        CASE WHEN time <= '2021-08-13' THEN 415110.00436772 ELSE 0 END AS reserve
    FROM date_series
)

SELECT 
    time,
    CAST(liq_incentives AS DOUBLE) AS liq_incentives,
    CAST(early_users AS DOUBLE) AS early_users,
    CAST(core_team AS DOUBLE) AS core_team,
    CAST(investors AS DOUBLE) AS investors,
    CAST(employees AS DOUBLE) AS employees,
    CAST(reserve AS DOUBLE) AS reserve
FROM blockworks_crv_supply_schedule
ORDER BY time;

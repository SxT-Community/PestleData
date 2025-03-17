SELECT 
    DATE(block_timestamp) AS date,
    SUM(amount_in_usd) AS total_amount_in_usd
FROM (
    SELECT * 
    FROM ethereum.defi.ez_dex_swaps
    WHERE platform = 'balancer'
) AS curve_swaps
GROUP BY date
ORDER BY date ASC;
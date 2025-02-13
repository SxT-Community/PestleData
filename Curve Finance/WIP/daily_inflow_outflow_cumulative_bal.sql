WITH daily_flows AS (
    SELECT 
        DATE(block_timestamp) AS tx_date,
        SUM(CASE WHEN lower(to_address) = '0x5f3b5dfeb7b28cdbd7faba78963ee202a494e2a2' THEN amount_usd ELSE 0 END) AS daily_inflow,
        SUM(CASE WHEN lower(from_address) = '0x5f3b5dfeb7b28cdbd7faba78963ee202a494e2a2' THEN amount_usd ELSE 0 END) AS daily_outflow
    FROM ethereum.core.ez_token_transfers
    WHERE lower(to_address) = '0x5f3b5dfeb7b28cdbd7faba78963ee202a494e2a2'
       OR lower(from_address) = '0x5f3b5dfeb7b28cdbd7faba78963ee202a494e2a2'
    GROUP BY tx_date
)
SELECT 
    tx_date,
    daily_inflow,
    daily_outflow,
    SUM(daily_inflow - daily_outflow) OVER (ORDER BY tx_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_balance
FROM daily_flows
ORDER BY tx_date;

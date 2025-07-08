WITH eigenpods AS (
    SELECT 
        decoded_log:eigenPod::STRING AS eigenpod
    FROM ethereum.core.ez_decoded_event_logs
    WHERE 
        LOWER(contract_address) = LOWER('0x91E677b07F7AF907ec9a428aafA9fc14a0d3A338') -- EigenPodManager
        AND event_name = 'PodDeployed'
),

deposits AS (
    SELECT 
        SUM(d.deposit_amount) AS total_eth_deposited
    FROM ethereum.beacon_chain.ez_deposits d
    INNER JOIN eigenpods ep 
        ON LOWER(d.withdrawal_address) = LOWER(ep.eigenpod)
),

withdrawals AS (
    SELECT 
        SUM(
            CASE 
                WHEN withdrawal_amount BETWEEN 20 AND 32 THEN withdrawal_amount
                WHEN withdrawal_amount > 32 THEN 32
                ELSE 0
            END
        ) AS total_eth_withdrawn
    FROM ethereum.beacon_chain.ez_withdrawals w
    INNER JOIN eigenpods ep 
        ON LOWER(w.withdrawal_address) = LOWER(ep.eigenpod)
),

net_eth_restaked AS (
    SELECT 
        COALESCE(d.total_eth_deposited, 0) - COALESCE(w.total_eth_withdrawn, 0) AS eth_restaked
    FROM deposits d, withdrawals w
)

SELECT 
    eth_restaked
FROM net_eth_restaked;

-- Master dashboard reference: https://dune.com/balancer/built-on-balancer
-- Reference query: https://dune.com/queries/3149076/5252771
-- What I derived from the above query https://dune.com/queries/4801020

-- Modified query works on Dune but not on Flipside
-- yet to figure out the tables and columns

WITH query_3144841 AS (
    WITH gyro_pools AS (
        SELECT r.poolId, 'arbitrum' AS blockchain, 
               COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) AS name, 
               'gyroscope' AS project
        FROM balancer_v2_arbitrum.Vault_evt_PoolRegistered r
        INNER JOIN labels.balancer_v2_pools_arbitrum l 
            ON r.poolAddress = l.address AND l.pool_type = 'ECLP'
        UNION ALL
        SELECT r.poolId, 'optimism' AS blockchain, 
               COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) AS name, 
               'gyroscope' AS project
        FROM balancer_v2_optimism.Vault_evt_PoolRegistered r
        INNER JOIN labels.balancer_v2_pools_optimism l 
            ON r.poolAddress = l.address AND l.pool_type = 'ECLP'
        UNION ALL
        SELECT r.poolId, 'ethereum' AS blockchain, 
               COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) AS name, 
               'gyroscope' AS project
        FROM balancer_v2_ethereum.Vault_evt_PoolRegistered r
        INNER JOIN labels.balancer_v2_pools_ethereum l 
            ON r.poolAddress = l.address AND l.pool_type = 'ECLP'
    ),
    beets_pools AS (
        SELECT r.poolId, 'optimism' AS blockchain, 
               COALESCE(l.name, CAST(BYTEARRAY_SUBSTRING(r.poolId,3,8) AS VARCHAR)) AS name, 
               'beethoven x' AS project
        FROM balancer_v2_optimism.Vault_evt_PoolRegistered r
        LEFT JOIN labels.balancer_v2_pools_optimism l 
            ON r.poolAddress = l.address
    )
    SELECT * FROM gyro_pools
    UNION ALL
    SELECT * FROM beets_pools
),
query_3147646 AS (
    WITH pools AS (
        SELECT * FROM query_3144841
    ),
    tvl AS (
        SELECT l.pool_id, BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address, l.blockchain, 
               SUM(l.protocol_liquidity_usd) AS tvl
        FROM beethoven_x_fantom.liquidity l
        WHERE l.protocol_liquidity_usd > 1 AND l.day = (CURRENT_DATE - INTERVAL '1' day)
        GROUP BY 1,2,3
        UNION ALL
        SELECT l.pool_id, BYTEARRAY_SUBSTRING(l.pool_id, 1, 20) AS pool_address, l.blockchain, 
               SUM(l.protocol_liquidity_usd) AS tvl
        FROM jelly_swap_sei.liquidity l
        WHERE l.protocol_liquidity_usd > 1 AND l.day = (CURRENT_DATE - INTERVAL '1' day)
        GROUP BY 1,2,3
    )
    SELECT * FROM tvl
)
SELECT DATE(t.block_time) AS trade_date, 
       SUM(t.amount_usd)/1e6 AS daily_volume
FROM balancer.trades t
LEFT JOIN query_3144841 q 
    ON t.project_contract_address = BYTEARRAY_SUBSTRING(q.poolId,1,20) 
    AND t.blockchain = q.blockchain
WHERE q.project IS NOT NULL
GROUP BY DATE(t.block_time)
ORDER BY DATE(t.block_time);
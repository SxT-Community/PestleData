WITH c AS (
    -- 'Completed' events from the Legacy Oracle
    SELECT
        block_timestamp AS evt_block_time,
        tx_hash AS evt_tx_hash,
        block_number AS evt_block_number,
        DECODED_LOG:value::NUMBER AS value  -- Extract 'value' parameter
    FROM ethereum.core.ez_decoded_event_logs
    WHERE LOWER(contract_address) = LOWER('0x442af784A788A5bd6F42A01Ebe9F287a871243fb')
      AND LOWER(event_name) = 'completed'
),

p AS (
    -- 'PostTotalShares' events from the Legacy Oracle
    SELECT
        block_timestamp AS evt_block_time,
        tx_hash AS evt_tx_hash,
        block_number AS evt_block_number,
        DECODED_LOG:postTotalPooledEther::NUMBER AS postTotalPooledEther,
        DECODED_LOG:preTotalPooledEther::NUMBER AS preTotalPooledEther
    FROM ethereum.core.ez_decoded_event_logs
    WHERE LOWER(contract_address) = LOWER('0x442af784A788A5bd6F42A01Ebe9F287a871243fb')
      AND LOWER(event_name) = 'posttotalshares'
),

t AS (
    -- Transfer events from stETH token
    SELECT
        block_timestamp AS evt_block_time,
        tx_hash AS evt_tx_hash,
        block_number AS evt_block_number,
        amount AS value  -- Amount transferred
    FROM ethereum.core.ez_token_transfers
    WHERE LOWER(contract_address) = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
),

ed AS (
    -- 'ETHDistributed' events from stETH token
    SELECT
        block_timestamp AS evt_block_time,
        tx_hash AS evt_tx_hash,
        block_number AS evt_block_number,
        DECODED_LOG:postCLBalance::NUMBER AS postCLBalance,
        DECODED_LOG:withdrawalsWithdrawn::NUMBER AS withdrawalsWithdrawn,
        DECODED_LOG:preCLBalance::NUMBER AS preCLBalance,
        DECODED_LOG:executionLayerRewardsWithdrawn::NUMBER AS executionLayerRewardsWithdrawn
    FROM ethereum.core.ez_decoded_event_logs
    WHERE LOWER(contract_address) = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
      AND LOWER(event_name) = 'ethdistributed'
)

, rebases AS (
    SELECT
        COALESCE(
            LAG(time) OVER (ORDER BY time NULLS FIRST),
            time - INTERVAL '24 HOURS'
        ) AS time,
        time AS next_time,
        rewards_paid,
        evt_tx_hash,
        evt_block_number
    FROM (
        SELECT
            c.evt_block_time AS time,
            SUM(t.value) * 9 AS rewards_paid,
            c.evt_tx_hash,
            c.evt_block_number
        FROM c
        JOIN t ON c.evt_tx_hash = t.evt_tx_hash
        WHERE c.evt_tx_hash NOT IN (SELECT evt_tx_hash FROM p)
        GROUP BY 1, 3, 4
    )

    UNION ALL

    SELECT
        COALESCE(
            LAG(evt_block_time) OVER (ORDER BY evt_block_time NULLS FIRST),
            evt_block_time - INTERVAL '24 HOURS'
        ) AS time,
        evt_block_time AS next_time,
        (postTotalPooledEther - preTotalPooledEther) * 0.9 AS rewards_paid,
        evt_tx_hash,
        evt_block_number
    FROM p
    WHERE evt_block_time <= DATE '2023-05-16'

    UNION ALL

    SELECT
        COALESCE(
            LAG(evt_block_time) OVER (ORDER BY evt_block_time NULLS FIRST),
            evt_block_time - INTERVAL '24 HOURS'
        ) AS time,
        evt_block_time AS next_time,
        0.9 * (postCLBalance + withdrawalsWithdrawn - preCLBalance + executionLayerRewardsWithdrawn) AS rewards_paid,
        evt_tx_hash,
        evt_block_number
    FROM ed
    WHERE evt_block_time > DATE '2023-05-16'
)

SELECT
    time,
    next_time,
    rewards_paid / 1e18 AS rewards_paid_daily,
    SUM(rewards_paid) OVER (ORDER BY time) / 1e18 AS rewards_paid_cumu,
    evt_tx_hash,
    evt_block_number
FROM rebases
ORDER BY 1 DESC;

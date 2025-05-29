WITH eigenlayer_ethereum__AVSDirectory_evt_OperatorAVSRegistrationStatusUpdated AS (
    SELECT 
        tx_hash, 
        event_index, 
        block_timestamp, 
        block_number,
        LOWER(decoded_log:avs::STRING) AS avs,
        LOWER(decoded_log:operator::STRING) AS operator,
        decoded_log:status::STRING AS status
    FROM ethereum.core.ez_decoded_event_logs
    WHERE LOWER(contract_address) = LOWER('0x135DDa560e946695d6f155dACaFC6f1F25C1F5AF') 
    AND event_name = 'OperatorAVSRegistrationStatusUpdated'
),

eigenlayer_ethereum__DelegationManager_evt_OperatorSharesIncreased AS (
    SELECT 
        tx_hash, 
        event_index, 
        block_timestamp, 
        block_number,
        LOWER(decoded_log:operator::STRING) AS operator,
        decoded_log:shares::NUMBER AS shares,
        LOWER(decoded_log:staker::STRING) AS staker,
        LOWER(decoded_log:strategy::STRING) AS strategy
    FROM ethereum.core.ez_decoded_event_logs
    WHERE LOWER(contract_address) = LOWER('0x39053d51b77dc0d36036fc1fcc8cb819df8ef37a')
    AND event_name = 'OperatorSharesIncreased'
),

eigenlayer_ethereum__DelegationManager_evt_OperatorSharesDecreased AS (
    SELECT 
        tx_hash, 
        event_index, 
        block_timestamp, 
        block_number,
        LOWER(decoded_log:operator::STRING) AS operator,
        decoded_log:shares::NUMBER AS shares,
        LOWER(decoded_log:staker::STRING) AS staker,
        LOWER(decoded_log:strategy::STRING) AS strategy
    FROM ethereum.core.ez_decoded_event_logs
    WHERE LOWER(contract_address) = LOWER('0x39053d51b77dc0d36036fc1fcc8cb819df8ef37a')
    AND event_name = 'OperatorSharesDecreased'
),

latest_status AS (
    SELECT
        lower(operator) as operator,
        lower(avs) as avs,
        status,
        ROW_NUMBER() OVER (PARTITION BY lower(operator), lower(avs) ORDER BY block_timestamp DESC) as rn
    FROM eigenlayer_ethereum__AVSDirectory_evt_OperatorAVSRegistrationStatusUpdated
),

avs_count AS (
    SELECT
        operator,
        sum(CASE WHEN status = 1 THEN 1 END) as avs_cnt
    FROM latest_status
    WHERE rn = 1
    GROUP BY operator
),

stakes AS (
    SELECT 
        lower(staker) as staker,
        lower(operator) as operator,
        SUM(CASE 
            WHEN LOWER(strategy) IN (
                LOWER('0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeebeac0'),
                LOWER('0x54945180db7943c0ed0fee7edab2bd24620256bc'),
                LOWER('0x93c4b944d05dfe6df7645a86cd2206016c51564d'),
                LOWER('0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2'),
                LOWER('0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d'),
                LOWER('0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff'),
                LOWER('0xa4c637e0f704745d182e4d38cab7e7485321d059'),
                LOWER('0x57ba429517c3473b6d34ca9acd56c0e735b94c02'),
                LOWER('0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6'),
                LOWER('0x7ca911e83dabf90c90dd3de5411a10f1a6112184'),
                LOWER('0x8ca7a5d6f3acd3a7a8bc468a8cd0fb14b6bd28b6'),
                LOWER('0xae60d8180437b5c34bb956822ac2710972584473'),
                LOWER('0x298afb19a105d59e74658c4c334ff360bade6dd2')
            ) THEN shares ELSE 0 END) / 1e18 as eth_stakes,
        SUM(CASE WHEN LOWER(strategy) = LOWER('0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7') THEN shares ELSE 0 END) / 1e18 as eigen_stakes
    FROM eigenlayer_ethereum__DelegationManager_evt_OperatorSharesIncreased
    GROUP BY lower(staker), lower(operator)
),

unstakes AS (
    SELECT 
        lower(staker) as staker,
        lower(operator) as operator,
        SUM(CASE 
            WHEN LOWER(strategy) IN (
                LOWER('0xbeac0eeeeeeeeeeeeeeeeeeeeeeeeeeebeac0'),
                LOWER('0x54945180db7943c0ed0fee7edab2bd24620256bc'),
                LOWER('0x93c4b944d05dfe6df7645a86cd2206016c51564d'),
                LOWER('0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2'),
                LOWER('0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d'),
                LOWER('0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff'),
                LOWER('0xa4c637e0f704745d182e4d38cab7e7485321d059'),
                LOWER('0x57ba429517c3473b6d34ca9acd56c0e735b94c02'),
                LOWER('0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6'),
                LOWER('0x7ca911e83dabf90c90dd3de5411a10f1a6112184'),
                LOWER('0x8ca7a5d6f3acd3a7a8bc468a8cd0fb14b6bd28b6'),
                LOWER('0xae60d8180437b5c34bb956822ac2710972584473'),
                LOWER('0x298afb19a105d59e74658c4c334ff360bade6dd2')

            ) THEN shares ELSE 0 END) / 1e18 as eth_unstakes,
        SUM(CASE WHEN LOWER(strategy) = LOWER('0xacb55c530acdb2849e6d4f36992cd8c9d50ed8f7') THEN shares ELSE 0 END) / 1e18 as eigen_unstakes
    FROM eigenlayer_ethereum__DelegationManager_evt_OperatorSharesDecreased
    GROUP BY lower(staker), lower(operator)
),

staker_tvl AS (
    SELECT
        COALESCE(s.staker, u.staker) as staker,
        COALESCE(s.operator, u.operator) as operator,
        COALESCE(s.eth_stakes, 0) - COALESCE(u.eth_unstakes, 0) as eth_tvl,
        COALESCE(s.eigen_stakes, 0) - COALESCE(u.eigen_unstakes, 0) as eigen_tvl
    FROM stakes s
    FULL OUTER JOIN unstakes u ON lower(s.staker) = lower(u.staker) AND lower(s.operator) = lower(u.operator)
),

filtered_staker_tvl AS (
    SELECT st.*
    FROM staker_tvl st
    JOIN avs_count ac ON lower(st.operator) = lower(ac.operator)
    WHERE ac.avs_cnt > 0
),

total_tvl AS (
    SELECT 
        SUM(eth_tvl) as total_eth_tvl,
        SUM(eigen_tvl) as total_eigen_tvl
    FROM filtered_staker_tvl
),

rewards_calculation AS (
    SELECT
        st.*,
        ac.avs_cnt,
        -- ETH and LST rewards (3% of total rewards for stakers)
        (st.eth_tvl / NULLIF(tt.total_eth_tvl, 0)) * (1287420 * 0.75) as eth_rewards,
        -- EIGEN rewards (1% of total rewards for stakers)
        (st.eigen_tvl / NULLIF(tt.total_eigen_tvl, 0)) * (1287420 * 0.25) as eigen_rewards
    FROM filtered_staker_tvl st
    CROSS JOIN total_tvl tt
    LEFT JOIN avs_count ac ON st.operator = ac.operator
)

SELECT
    rc.staker,
    rc.operator,
    rc.avs_cnt as operator_avs_count,
    rc.eth_tvl as delegated_eth,
    rc.eth_rewards as rewards_for_eth,
    rc.eigen_tvl as delegated_eigen,
    rc.eigen_rewards as rewards_for_eigen,
    rc.eth_rewards + rc.eigen_rewards as total_rewards,
    (rc.eth_rewards + rc.eigen_rewards) * 0.1 as commission_to_operator,
    (rc.eth_rewards + rc.eigen_rewards) * 0.9 as total_staker_reward_with_commission
FROM rewards_calculation rc
WHERE (rc.eth_tvl > 0 OR rc.eigen_tvl > 0) 
ORDER BY total_rewards DESC


--ref dune query- https://dune.com/queries/4127229/6949404
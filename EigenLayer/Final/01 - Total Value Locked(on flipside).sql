WITH token_balances AS (
    SELECT 
        CASE LOWER(contract_address)
            WHEN LOWER('0xBe9895146f7AF43049ca1c1AE358B0541Ea49704') THEN 'CBETH'
            WHEN LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84') THEN 'STETH'
            WHEN LOWER('0xae78736Cd615f374D3085123A210448E74Fc6393') THEN 'RETH'
            WHEN LOWER('0xa2E3356610840701BDf5611a53974510Ae27E2e1') THEN 'WBETH'
            WHEN LOWER('0xf951E335afb289353dc249e82926178EaC7DEd78') THEN 'SWETH'
            WHEN LOWER('0xf1C9acDc66974dFB6dEcB12aA385b9cD01190E38') THEN 'OSETH'
            WHEN LOWER('0x856c4Efb76C1D1AE02e20CEB03A2A6a08b0b8dC3') THEN 'OETH'
            WHEN LOWER('0xE95A203B1a91a908F9B9CE46459d101078c2c3cb') THEN 'ANKRETH'
            WHEN LOWER('0xA35b1B31Ce002FBF2058D22F30f95D405200A15b') THEN 'ETHX'
            WHEN LOWER('0xd5F7838F5C461fefF7FE49ea5ebaF7728bB0ADfa') THEN 'METH'
            WHEN LOWER('0x8c1BEd5b9a0928467c9B1341Da1D7BD5e10b6549') THEN 'LSETH'
            WHEN LOWER('0xac3E018457B222d93114458476f3E3416Abbe38F') THEN 'SFRXETH'
        END AS symbol,
        balance / 1e18 AS balance,
        user_address AS address,
        ROW_NUMBER() OVER (
            PARTITION BY LOWER(contract_address)
            ORDER BY block_timestamp DESC
        ) AS rn
    FROM ethereum.core.fact_token_balances
    WHERE LOWER(user_address) IN (
        LOWER('0x54945180db7943c0ed0fee7edab2bd24620256bc'),
        LOWER('0x93c4b944d05dfe6df7645a86cd2206016c51564d'),
        LOWER('0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2'),
        LOWER('0x7ca911e83dabf90c90dd3de5411a10f1a6112184'),
        LOWER('0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6'),
        LOWER('0x57ba429517c3473b6d34ca9acd56c0e735b94c02'),
        LOWER('0xa4c637e0f704745d182e4d38cab7e7485321d059'),
        LOWER('0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff'),
        LOWER('0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d'),
        LOWER('0x298afb19a105d59e74658c4c334ff360bade6dd2'),
        LOWER('0xae60d8180437b5c34bb956822ac2710972584473'),
        LOWER('0x8ca7a5d6f3acd3a7a8bc468a8cd0fb14b6bd28b6')
    )
    AND LOWER(contract_address) IN (
        LOWER('0xBe9895146f7AF43049ca1c1AE358B0541Ea49704'),
        LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84'),
        LOWER('0xae78736Cd615f374D3085123A210448E74Fc6393'),
        LOWER('0xa2E3356610840701BDf5611a53974510Ae27E2e1'),
        LOWER('0xf951E335afb289353dc249e82926178EaC7DEd78'),
        LOWER('0xf1C9acDc66974dFB6dEcB12aA385b9cD01190E38'),
        LOWER('0x856c4Efb76C1D1AE02e20CEB03A2A6a08b0b8dC3'),
        LOWER('0xE95A203B1a91a908F9B9CE46459d101078c2c3cb'),
        LOWER('0xA35b1B31Ce002FBF2058D22F30f95D405200A15b'),
        LOWER('0xd5F7838F5C461fefF7FE49ea5ebaF7728bB0ADfa'),
        LOWER('0x8c1BEd5b9a0928467c9B1341Da1D7BD5e10b6549'),
        LOWER('0xac3E018457B222d93114458476f3E3416Abbe38F')
    )
),

latest_token_balances AS (
    SELECT symbol,balance
    FROM token_balances
    WHERE rn = 1
),

eigenpods AS (
    SELECT decoded_log:eigenPod::STRING AS eigenpod
    FROM ethereum.core.ez_decoded_event_logs
    WHERE LOWER(contract_address) = LOWER('0x91E677b07F7AF907ec9a428aafA9fc14a0d3A338')
      AND event_name = 'PodDeployed'
),

deposits AS (
    SELECT SUM(d.deposit_amount) AS amount_deposited
    FROM ethereum.beacon_chain.ez_deposits d
    INNER JOIN eigenpods p ON LOWER(d.withdrawal_address) = LOWER(p.eigenpod)
),

withdrawals AS (
    SELECT SUM(
        CASE 
            WHEN withdrawal_amount BETWEEN 20 AND 32 THEN withdrawal_amount 
            WHEN withdrawal_amount > 32 THEN 32
            ELSE 0
        END
    ) AS amount_withdrawn
    FROM ethereum.beacon_chain.ez_withdrawals w
    INNER JOIN eigenpods p ON LOWER(w.withdrawal_address) = LOWER(p.eigenpod)
),

net_eth_staked AS (
  SELECT 
    COALESCE((SELECT amount_deposited FROM deposits), 0) - 
    COALESCE((SELECT amount_withdrawn FROM withdrawals), 0) AS net_eth_staked
),

eth_balance AS (
    SELECT 'ETH' AS symbol, net_eth_staked AS balance
    FROM net_eth_staked
),

latest_prices AS (
    SELECT symbol, MAX_BY(price, hour) AS price
    FROM ethereum.price.ez_prices_hourly
    GROUP BY symbol
),

ETH_price AS (
    SELECT price AS eth_price
    FROM latest_prices
    WHERE symbol = 'ETH'
),

TVL_calculation AS (
    SELECT 
        f.symbol,
        f.balance,
        f.balance * p.price AS TVL
    FROM (
        SELECT * FROM latest_token_balances
        UNION ALL
        SELECT * FROM eth_balance
    ) f
    LEFT JOIN latest_prices p ON f.symbol = p.symbol
),

final_comparison AS (
    SELECT
        'Native' AS category,
        SUM(CASE WHEN symbol = 'ETH' THEN TVL ELSE 0 END) AS tvl_usd
    FROM TVL_calculation
    
    UNION ALL
    
    SELECT
        'LST' AS category,
        SUM(CASE WHEN symbol != 'ETH' AND TVL IS NOT NULL THEN TVL ELSE 0 END) AS tvl_usd
    FROM TVL_calculation
)

SELECT
    category,
    tvl_usd,
    tvl_usd / (SELECT eth_price FROM ETH_price) AS tvl_eth
FROM final_comparison
ORDER BY category DESC
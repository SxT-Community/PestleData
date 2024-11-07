WITH daily_snapshots AS (
    SELECT 
        BLOCK_TIMESTAMP,
        BALANCE,
        CONTRACT_ADDRESS,
        USER_ADDRESS,
        ROW_NUMBER() OVER (
            PARTITION BY CONTRACT_ADDRESS, 
                         DATE_TRUNC('day', BLOCK_TIMESTAMP)
            ORDER BY BLOCK_TIMESTAMP DESC
        ) AS row_num
    FROM ethereum.core.fact_token_balances
    WHERE (
        lower(CONTRACT_ADDRESS) = lower('0xe65cdB6479BaC1e22340E4E755fAE7E509EcD06c')
        --cAAVE
        OR lower(CONTRACT_ADDRESS) = lower('0x6C8c6b02E7b2BE14d4fA6022Dfd6d75921D90E4E')
        --cBAT 
        OR lower(CONTRACT_ADDRESS) = lower('0x70e36f6BF80a52b3B46b3aF8e106CC0ed743E8e4')
        --cCOMP
        OR lower(CONTRACT_ADDRESS) = lower('0x5d3a536E4D6DbD6114cc1Ead35777bAB948E3643')
        --cDAI
        OR lower(CONTRACT_ADDRESS) = lower('0x4Ddc2D193948926D02f9B1fE9e1daa0718270ED5')
        --cETH
        OR lower(CONTRACT_ADDRESS) = lower('0x7713DD9Ca933848F6819F38B8352D9A15EA73F67')
        --cFEI
        OR lower(CONTRACT_ADDRESS) = lower('0xFAce851a4921ce59e912d19329929CE6da6EB0c7')
        --cLINK 
        OR lower(CONTRACT_ADDRESS) = lower('0x95b4eF2869eBD94BEb4eEE400a99824BF5DC325b')
        --cMKR
        OR lower(CONTRACT_ADDRESS) = lower('0x158079Ee67Fce2f58472A96584A73C7Ab9AC95c1')
        --cREP
        OR lower(CONTRACT_ADDRESS) = lower('0xF5DCe57282A584D2746FaF1593d3121Fcac444dC')
        --cSAI

        OR lower(CONTRACT_ADDRESS) = lower('0x4B0181102A0112A2ef11AbEE5563bb4a3176c9d7')
        --cSUSHI
        OR lower(CONTRACT_ADDRESS) = lower('0x12392F67bdf24faE0AF363c24aC620a2f67DAd86')
        --cTUSD 
        OR lower(CONTRACT_ADDRESS) = lower('0x35A18000230DA775CAc24873d00Ff85BccdeD550')
        --cUNI
        OR lower(CONTRACT_ADDRESS) = lower('0x39AA39c021dfbaE8faC545936693aC917d5E7563')
        --cUSDC
        OR lower(CONTRACT_ADDRESS) = lower('0x041171993284df560249B57358F931D9eB7b925D')
        --cUSDP
        OR lower(CONTRACT_ADDRESS) = lower('0xf650C3d88D12dB855b8bf7D11Be6C55A4e07dCC9')
        --cUSDT
        OR lower(CONTRACT_ADDRESS) = lower('0xC11b1268C1A384e55C48c2391d8d480264A3A7F4')
        --cWBTC 
        OR lower(CONTRACT_ADDRESS) = lower('0xccF4429DB6322D5C611ee964527D42E5d685DD6a')
        --cWBTC2
        OR lower(CONTRACT_ADDRESS) = lower('0x80a2AE356fc9ef4305676f7a3E2Ed04e12C33946')
        --cYFI
        OR lower(CONTRACT_ADDRESS) = lower('0xB3319f5D18Bc0D84dD1b4825Dcde5d5f7266d407')
        --cZRX

        OR lower(CONTRACT_ADDRESS) = lower('0xc00e94Cb662C3520282E6f5717214004A7f26888')
        --COMP
        OR lower(CONTRACT_ADDRESS) = lower('0x3d9819210A31b4961b30EF54bE2aeD79B9c9Cd3B')
        --Comptroller 
        OR lower(CONTRACT_ADDRESS) = lower('0xc0Da02939E1441F497fd74F78cE7Decb17B66529')
        --Governance
        OR lower(CONTRACT_ADDRESS) = lower('0x6d903f6003cca6255D85CcA4D3B5E5146dC33925')
        --Timelock
    )
    AND lower(USER_ADDRESS) = lower('0xc00e94cb662c3520282e6f5717214004a7f26888')
    --Compound v2 contract address
)
SELECT cast(BLOCK_TIMESTAMP as date) as BLOCK_DATE, 
BALANCE, CONTRACT_ADDRESS, USER_ADDRESS
FROM daily_snapshots
WHERE row_num = 1
ORDER BY 1
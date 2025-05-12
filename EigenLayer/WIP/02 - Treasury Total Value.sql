SELECT
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE to_address = LOWER('0x93c4b944D05dfe6df7645A86cd2206016c51564D')
      AND contract_address = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
  ), 0) -
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE from_address = LOWER('0x93c4b944D05dfe6df7645A86cd2206016c51564D')
      AND contract_address = LOWER('0xae7ab96520DE3A18E5e111B5EaAb095312D7fE84')
  ), 0)
  
  + COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE to_address = LOWER('0x54945180dB7943c0ed0FEE7EdaB2Bd24620256bc')
      AND contract_address = LOWER('0xBe9895146f7AF43049ca1c1AE358B0541Ea49704')
  ), 0) -
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE from_address = LOWER('0x54945180dB7943c0ed0FEE7EdaB2Bd24620256bc')
      AND contract_address = LOWER('0xBe9895146f7AF43049ca1c1AE358B0541Ea49704')
  ), 0)

  + COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE to_address = LOWER('0x1BeE69b7dFFfA4E2d53C2a2Df135C388AD25dCD2')
      AND contract_address = LOWER('0xae78736Cd615f374D3085123A210448E74Fc6393')
  ), 0) -
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE from_address = LOWER('0x1BeE69b7dFFfA4E2d53C2a2Df135C388AD25dCD2')
      AND contract_address = LOWER('0xae78736Cd615f374D3085123A210448E74Fc6393')
  ), 0)

  + COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE to_address = LOWER('0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6')
      AND contract_address = LOWER('0xf951E335afb289353dc249e82926178EaC7DEd78')
  ), 0) -
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE from_address = LOWER('0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6')
      AND contract_address = LOWER('0xf951E335afb289353dc249e82926178EaC7DEd78')
  ), 0)

  + COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE to_address = LOWER('0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d')
      AND contract_address = LOWER('0xA35b1B31Ce002FBF2058D22F30f95D405200A15b')
  ), 0) -
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE from_address = LOWER('0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d')
      AND contract_address = LOWER('0xA35b1B31Ce002FBF2058D22F30f95D405200A15b')
  ), 0)

  + COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE to_address = LOWER('0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff')
      AND contract_address = LOWER('0xE95A203B1a91a908F9B9CE46459d101078c2c3cb')
  ), 0) -
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE from_address = LOWER('0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff')
      AND contract_address = LOWER('0xE95A203B1a91a908F9B9CE46459d101078c2c3cb')
  ), 0)

  + COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE to_address = LOWER('0x57ba429517c3473b6d34ca9acd56c0e735b94c02')
      AND contract_address = LOWER('0xf1C9acDc66974dFB6dEcB12aA385b9cD01190E38')
  ), 0) -
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE from_address = LOWER('0x57ba429517c3473b6d34ca9acd56c0e735b94c02')
      AND contract_address = LOWER('0xf1C9acDc66974dFB6dEcB12aA385b9cD01190E38')
  ), 0)

  + COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE to_address = LOWER('0xa4c637e0f704745d182e4d38cab7e7485321d059')
      AND contract_address = LOWER('0x856c4Efb76C1D1AE02e20CEB03A2A6a08b0b8dC3')
  ), 0) -
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE from_address = LOWER('0xa4c637e0f704745d182e4d38cab7e7485321d059')
      AND contract_address = LOWER('0x856c4Efb76C1D1AE02e20CEB03A2A6a08b0b8dC3')
  ), 0)

  + COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE to_address = LOWER('0x7ca911e83dabf90c90dd3de5411a10f1a6112184')
      AND contract_address = LOWER('0xa2E3356610840701BDf5611a53974510Ae27E2e1')
  ), 0) -
  COALESCE((
    SELECT SUM(raw_amount / 1e18)
    FROM ethereum.core.ez_token_transfers
    WHERE from_address = LOWER('0x7ca911e83dabf90c90dd3de5411a10f1a6112184')
      AND contract_address = LOWER('0xa2E3356610840701BDf5611a53974510Ae27E2e1')
  ), 0)
  AS total_net_tokens;
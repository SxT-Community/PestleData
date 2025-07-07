WITH airdrop_claims AS (
  SELECT
    sum(decoded_log:amount::FLOAT / 1e18) AS amount_eigen
  FROM ETHEREUM_ONCHAIN_CORE_DATA.CORE.EZ_DECODED_EVENT_LOGS
  WHERE
    tx_succeeded = 'TRUE'
    AND LOWER(contract_address) IN (
      '0x035bdaeab85e47710c27eda7fd754ba80ad4ad02',  -- Season 1 Distributor
      '0xf532a5a35007804a9ca79e7fa15d8f648f6d7f28',  -- Season 1 Phase 2 Distributor
      '0x2ec90ef34e312a855becf74762d198d8369eece1',
      lower('0xa105C3AbeDBAf4295AC6149BF24D5311F629934c')-- Season 2 Distributor
    )
    AND event_name in ('Claimed')
),

rewards_claimed AS (
  SELECT
    decoded_log:token::STRING AS token_address,
    SUM(decoded_log:claimedAmount::FLOAT) / 1e18 AS token_amount
  FROM ETHEREUM_ONCHAIN_CORE_DATA.CORE.EZ_DECODED_EVENT_LOGS
  WHERE 
    LOWER(contract_address) = LOWER('0x7750d328b314effa365a0402ccfd489b80b0adda')  
    AND event_name = 'RewardsClaimed'
    AND LOWER(decoded_log:token::STRING) = LOWER('0xec53bf9167f50cdeb3ae105f56099aaab9061f83')  
  GROUP BY token_address
)

SELECT
  amount_eigen + token_amount  AS circulating_eigen
FROM airdrop_claims,rewards_claimed

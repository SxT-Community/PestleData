WITH supply_minted_data AS (
  SELECT
    CAST(decoded_log:supplyMinted AS DECIMAL(38,0)) / 1e18 AS minted,
    SUM(CAST(decoded_log:supplyMinted AS DECIMAL(38,0))) 
      OVER (ORDER BY block_timestamp) / 1e18 
      + 318950466.088773603337882496 - 164343085.67598772 AS TOTAL_SUPPLY,
    block_timestamp,
    tx_hash
  FROM
    ethereum.core.ez_decoded_event_logs
  WHERE
    (contract_address = LOWER('0x8d203C458d536Fe0F97e9f741bC231EaC8cd91cf') or contract_address = LOWER('0xA05e45396703BabAa9C276B5E5A9B6e2c175b521'))
    AND LOWER(event_name) = 'supplyminted'
  ORDER BY
    block_timestamp DESC
)

SELECT
  minted / TOTAL_SUPPLY * 52 AS apr,
  minted / TOTAL_SUPPLY AS percent,
  *
FROM
  supply_minted_data;

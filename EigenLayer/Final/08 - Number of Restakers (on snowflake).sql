WITH combined_events AS (
    SELECT 
        "DECODED_LOG":"staker"::STRING AS staker,
        "DECODED_LOG":"shares"::FLOAT AS shares,
        "EVENT_NAME" AS event_name
    FROM ETHEREUM_ONCHAIN_CORE_DATA.CORE.EZ_DECODED_EVENT_LOGS
    WHERE LOWER("CONTRACT_ADDRESS") = LOWER('0x39053D51B77DC0d36036Fc1fCc8Cb819df8Ef37A')
      AND "EVENT_NAME" IN ('OperatorSharesIncreased', 'OperatorSharesDecreased')
)

SELECT 
    COUNT(DISTINCT staker)  AS number_of_stakers
FROM (
    SELECT 
        staker,
        SUM(
            CASE 
                WHEN event_name = 'OperatorSharesIncreased' THEN shares
                WHEN event_name = 'OperatorSharesDecreased' THEN -shares
            END
        ) AS net_shares
    FROM combined_events
    GROUP BY staker
    HAVING SUM(
        CASE 
            WHEN event_name = 'OperatorSharesIncreased' THEN shares
            WHEN event_name = 'OperatorSharesDecreased' THEN -shares
        END
    ) > 0
) net_stakers;

--source-https://economy.eigenlayer.xyz/

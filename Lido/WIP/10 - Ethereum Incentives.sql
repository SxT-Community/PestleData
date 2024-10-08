
/*
Look for topic0: 0x92dd3cb149a1eebd51fd8c2a3653fd96f30c4ac01d4f850fc16d46abd6c3e92f on STETH token.  Parse the result, and sum the RewardsWithdrawn and executionLayerRewardsWithdrawn fields.
Lido protocol start date:  2020-11-01
*/

with ETHEREUM_INCENTIVES_m10 as (
Select cast(Block_timestamp as date) as Block_Date, contract_address
, sum(coalesce(decoded_log['RewardsWithdrawn'],0))/1e18 as RewardsWithdrawn
, sum(coalesce(decoded_log['executionLayerRewardsWithdrawn'],0))/1e18 as executionLayerRewardsWithdrawn
, RewardsWithdrawn+executionLayerRewardsWithdrawn as Total_Eth_Incentives
from ethereum.core.ez_decoded_event_logs
where Block_Timestamp between cast('2020-11-01' as date) and current_date
and contract_address = '0xae7ab96520de3a18e5e111b5eaab095312d7fe84'
and topics[0] = '0x92dd3cb149a1eebd51fd8c2a3653fd96f30c4ac01d4f850fc16d46abd6c3e92f'
and (  NOT IS_NULL_VALUE(decoded_log:executionLayerRewardsWithdrawn)
    or NOT IS_NULL_VALUE(decoded_log:RewardsWithdrawn) )
group by 1,2
)
Select sum(Total_Eth_Incentives) as Total_Eth_Incentives
from ETHEREUM_INCENTIVES_m10

/*
Column RewardsWithdrawn  never returns  a non-zero value - correct key name?
Above query returns 112366.573 Eth
*/
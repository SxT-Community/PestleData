Flipside results not matching smart contract -  have reached out to Lido

/*
Should match the upper-right number on this dashboard:
https://dune.com/LidoAnalytical/Lido-Finance-Extended
Lido protocol start date:  2020-11-01
Number to match as of Sept 23, 2024:  9,741,325
*/

select (select sum
(ETH_AMOUNT_UNADJ) from ethereum.defi.ez_liquid_staking_deposits)
- (select sum
(ETH_AMOUNT_UNADJ) from ethereum.defi.ez_liquid_staking_withdrawals) as diff

select count(distinct user_address) as Token_holders
from ethereum.core.ez_current_balances
where lower(contract_address)=lower('0xec53bF9167f50cDEB3Ae105f56099aaaB9061F83')
and current_bal>0

-- Source- https://economy.eigenlayer.xyz/

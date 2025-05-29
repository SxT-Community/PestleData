/* 304M == right answer in aggregate, need to find way to spread over days */
select 
  TOKEN_NAME
, cast(LAST_ACTIVITY_BLOCK_TIMESTAMP as date) as Supply_Date
, SUM(CURRENT_BAL) AS Circulating_Supply
from ethereum.core.ez_current_balances
where TOKEN_NAME = 'EIGEN'
  and last_activity_block_timestamp::date between current_date-365 and current_date
GROUP BY 1,2
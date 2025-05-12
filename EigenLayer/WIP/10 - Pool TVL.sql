 with pricet as (
 SELECT
 HOUR::date as dates,
 SYMBOL,
 avg(price) as usd
 from ethereum.price.ez_hourly_token_prices
 where SYMBOL in ('rETH','stETH','cbETH')
 group by 1,2
 ),

total as (
select 
--t.symbol,
  date_trunc('day', BLOCK_TIMESTAMP) as days, 
sum(RAW_AMOUNT) / 1e18 as amt , sum(RAW_AMOUNT * USD) / 1e18 as usd_amt 
from ethereum.core.ez_token_transfers t 
join pricet p on t.BLOCK_TIMESTAMP::date=p.dates and t.symbol=p.symbol
where BLOCK_NUMBER >= 17445564 -- Created
and to_address ilike '0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2' -- rETH Pool
and contract_address ilike '0xae78736cd615f374d3085123a210448e74fc6393' -- rETH
group by 1

union

select
--t.symbol, 
  date_trunc('day', BLOCK_TIMESTAMP) as days, 
sum(RAW_AMOUNT) / 1e18 as amt , sum(RAW_AMOUNT * USD) / 1e18 as usd_amt 
from ethereum.core.ez_token_transfers t 
join pricet p on t.BLOCK_TIMESTAMP::date=p.dates and t.symbol=p.symbol
where BLOCK_NUMBER >= 17445564 -- Created
and to_address = '0x93c4b944d05dfe6df7645a86cd2206016c51564d' -- stETH Pool
and contract_address = '0xae7ab96520de3a18e5e111b5eaab095312d7fe84' -- stETH
group by 1

union 

   select 
--t.symbol,
  date_trunc('day', BLOCK_TIMESTAMP) as days, 
sum(RAW_AMOUNT) / 1e18 as amt , sum(RAW_AMOUNT * USD) / 1e18 as usd_amt 
from ethereum.core.ez_token_transfers t 
join pricet p on t.BLOCK_TIMESTAMP::date=p.dates and t.symbol=p.symbol
where BLOCK_NUMBER >= 17445564 -- Created
and to_address = '0x54945180db7943c0ed0fee7edab2bd24620256bc' -- cbETH Pool
and contract_address = '0xbe9895146f7af43049ca1c1ae358b0541ea49704' -- cbETH
group by 1

UNION

select 
--t.symbol,
  date_trunc('day', BLOCK_TIMESTAMP) as days,
(-1) * sum(RAW_AMOUNT) / 1e18 as amt , (-1) *sum(RAW_AMOUNT * USD) / 1e18 as usd_amt 
from ethereum.core.ez_token_transfers t 
join pricet p on t.BLOCK_TIMESTAMP::date=p.dates and t.symbol=p.symbol
where BLOCK_NUMBER >= 17445564 -- Created
and FROM_ADDRESS = '0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2' -- rETH Pool
and contract_address = '0xae78736cd615f374d3085123a210448e74fc6393' -- rETH
group by 1

UNION

select 
--t.symbol,
  date_trunc('day', BLOCK_TIMESTAMP) as days, 
(-1) * sum(RAW_AMOUNT) / 1e18 as amt , (-1) *sum(RAW_AMOUNT * USD) / 1e18 as usd_amt 
from ethereum.core.ez_token_transfers t 
join pricet p on t.BLOCK_TIMESTAMP::date=p.dates and t.symbol=p.symbol
where BLOCK_NUMBER >= 17445564 -- Created
and FROM_ADDRESS = '0x93c4b944d05dfe6df7645a86cd2206016c51564d' -- stETH Pool
and contract_address = '0xae7ab96520de3a18e5e111b5eaab095312d7fe84' -- stETH
group by 1

UNION

select 
--t.symbol,
  date_trunc('day', BLOCK_TIMESTAMP) as days, 
(-1) * sum(RAW_AMOUNT) / 1e18 as amt , (-1) *sum(RAW_AMOUNT * USD) / 1e18 as usd_amt 
from ethereum.core.ez_token_transfers t 
join pricet p on t.BLOCK_TIMESTAMP::date=p.dates and t.symbol=p.symbol
where BLOCK_NUMBER >= 17445564 -- Created
and FROM_ADDRESS = '0x54945180db7943c0ed0fee7edab2bd24620256bc' -- cbETH Pool
and contract_address = '0xbe9895146f7af43049ca1c1ae358b0541ea49704' -- cbETH
group by 1
)

SELECT
--distinct symbol,
days,
round(sum(amt),2) as net_amt,
round(sum(usd_amt),2) as net_usd_amt,
sum(net_amt) over (order by days asc) as tvl,
sum(net_usd_amt) over (order by days asc) as usd_tvl
from total
group by 1

 


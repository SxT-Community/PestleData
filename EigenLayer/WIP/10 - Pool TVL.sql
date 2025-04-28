EigenLayer Pool TVL-
LSD replicated(https://dune.com/queries/2648012)-
WITH lsd AS (
    SELECT column1 AS contract_address FROM VALUES
        ('0xae7ab96520de3a18e5e111b5eaab095312d7fe84'),
        ('0xac3e018457b222d93114458476f3e3416abbe38f'),
        ('0xfe2e637202056d30016725477c5da089ab0a043a'),
        ('0xf1c9acdc66974dfb6decb12aa385b9cd01190e38'),
        ('0xae78736cd615f374d3085123a210448e74fc6393'),
        ('0xe95a203b1a91a908f9b9ce46459d101078c2c3cb'),
        ('0xbe9895146f7af43049ca1c1ae358b0541ea49704'),
        ('0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3'),
        ('0xf951e335afb289353dc249e82926178eac7ded78'),
        ('0x9559aaa82d9649c7a7b220e7c461d2e74c9a3593'),
        ('0xc3501eaaf5e29aa0529342f409443120fd545ea7'),
        ('0xa2e3356610840701bdf5611a53974510ae27e2e1'),
        ('0x898bad2774eb97cf6b94605677f43b41871410b1'),
        ('0x3802c218221390025bceabbad5d8c59f40eb74b8'),
        ('0x5bbe36152d3cd3eb7183a82470b39b29eedf068b'),
        ('0xc6572019548dfeba782ba5a2093c836626c7789a'),
        ('0xcbc1065255cbc3ab41a6868c22d1f1c573ab89fd'),
        ('0xf1376bcef0f78459c0ed0ba5ddce976f1ddf51f4'),
        ('0xa35b1b31ce002fbf2058d22f30f95d405200a15b'),
        ('0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'),
        ('0xd5f7838f5c461feff7fe49ea5ebaf7728bb0adfa'),
        ('0x7122985656e38bdc0302db86685bb972b145bd3c'),
        ('0x24ae2da0f361aa4be46b48eb19c91e02c5e4f27e'),
        ('0x8c1bed5b9a0928467c9b1341da1d7bd5e10b6549'),
        ('0xfe18ae03741a5b84e39c295ac9c856ed7991c38e'),
        ('0x04c154b66cb340f3ae24111cc767e0184ed00cc6')
),
-- base metadata
token_decimal AS (
    SELECT DISTINCT contract_address, decimals, symbol
    FROM ethereum.core.ez_token_transfers
    WHERE contract_address IN (SELECT contract_address FROM lsd)
    
    UNION ALL
    
    SELECT * FROM VALUES 
        ('0xa35b1b31ce002fbf2058d22f30f95d405200a15b', 18, 'ETHx'),
        ('0xc6572019548dfeba782ba5a2093c836626c7789a', 18, 'nETH'),
        ('0xf1c9acdc66974dfb6decb12aa385b9cd01190e38', 18, 'osETH'),
        ('0xd5f7838f5c461feff7fe49ea5ebaf7728bb0adfa', 18, 'mETH'),
        ('0x7122985656e38bdc0302db86685bb972b145bd3c', 18, 'STONE'),
        ('0x24ae2da0f361aa4be46b48eb19c91e02c5e4f27e', 18, 'mevETH'),
        ('0x8c1bed5b9a0928467c9b1341da1d7bd5e10b6549', 18, 'LsETH'),
        ('0xfe18ae03741a5b84e39c295ac9c856ed7991c38e', 18, 'CDCETH'),
        ('0x04c154b66cb340f3ae24111cc767e0184ed00cc6', 18, 'pxETH')
),
-- time dimension
calendar AS (
    SELECT DATEADD(DAY, SEQ4(), DATE '2020-12-13') AS day
    FROM TABLE(GENERATOR(ROWCOUNT => 10000))
    WHERE DATEADD(DAY, SEQ4(), DATE '2020-12-13') <= CURRENT_DATE()
),
time AS (
    SELECT day, td.contract_address, td.symbol
    FROM calendar c
    JOIN (SELECT DISTINCT contract_address, symbol FROM token_decimal) td
),
-- fetch DEX pricing
dex_price AS (
    SELECT
        block_timestamp,
        CASE
            WHEN token_out IN (SELECT contract_address FROM lsd) THEN token_out
            ELSE token_in
        END AS contract_address,
        b.symbol,
        CASE
            WHEN token_out IN (SELECT contract_address FROM lsd)
                THEN AMOUNT_IN_USD / (CAST(AMOUNT_OUT_UNADJ AS FLOAT) / POWER(10, b.decimals))
            ELSE AMOUNT_IN_USD / (CAST(AMOUNT_IN_UNADJ AS FLOAT) / POWER(10, b.decimals))
        END AS price
    FROM ethereum.defi.ez_dex_swaps a
    LEFT JOIN token_decimal b ON a.token_out = b.contract_address OR a.token_in = b.contract_address
    WHERE token_out IN (SELECT contract_address FROM lsd)
       OR token_in IN (SELECT contract_address FROM lsd)
),
-- final daily price computation
final AS (
    SELECT
        day,
        contract_address,
        symbol,
        FIRST_VALUE(price) OVER (PARTITION BY group_num, symbol, contract_address ORDER BY day) AS price
    FROM (
        SELECT
            day,
            contract_address,
            symbol,
            price,
            SUM(CASE WHEN price IS NOT NULL THEN 1 ELSE 0 END)
                OVER (PARTITION BY contract_address, symbol ORDER BY day) AS group_num
        FROM (
            SELECT
                t.day,
                t.contract_address,
                t.symbol,
                CASE
                    WHEN AVG(d.price) IS NULL OR AVG(d.price) > 5000 THEN NULL
                    ELSE AVG(d.price)
                END AS price
            FROM time t
            LEFT JOIN dex_price d
                ON t.day = DATE_TRUNC('DAY', d.block_timestamp)
                AND t.contract_address = d.contract_address
            WHERE t.contract_address NOT IN (
                '0xae7ab96520de3a18e5e111b5eaab095312d7fe84',
                '0xae78736cd615f374d3085123a210448e74fc6393',
                '0xbe9895146f7af43049ca1c1ae358b0541ea49704',
                '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'
            )
            GROUP BY 1, 2, 3
        )
    )

    UNION ALL

    SELECT
        DATE_TRUNC('DAY', hour) AS day,
        token_address AS contract_address,
        symbol,
        AVG(price) AS price
    FROM ethereum.price.ez_prices_hourly
    WHERE token_address IN (
        '0xae7ab96520de3a18e5e111b5eaab095312d7fe84',
        '0xae78736cd615f374d3085123a210448e74fc6393',
        '0xbe9895146f7af43049ca1c1ae358b0541ea49704',
        '0x7f39c581f595b53c5cb19bd0b3f8da6c935e2ca0'
    )
    GROUP BY 1, 2, 3
)

SELECT * FROM final WHERE price IS NOT NULL order by day desc;


Query-3552897

with 
eigen_address as (
select distinct concat('0x',substr(cast(topic_1 as varchar),27,64)) as to_
from ethereum.core.ez_decoded_event_logs
where lower(contract_address) = lower('0x91E677b07F7AF907ec9a428aafA9fc14a0d3A338')
and lower(topic_0) = lower('0x21c99d0db02213c32fff5b05cf0a718ab5f858802b91498f80d82270289d856a')
),

eigen_balance_eth as (
select day, sum(amount) as netflow from (
select date_trunc('day', block_timestamp) as day, sum(deposit_amount) as amount
from ethereum.beacon_chain.ez_deposits
where cast(depositor as varchar) in (select to_ from eigen_address)
or cast(withdrawal_address as varchar) in (select to_ from eigen_address)
group by 1
union all
select 
date_trunc('day', block_timestamp) as day, sum(case when withdrawal_amount>=32 then floor(withdrawal_amount)*-1 else 0 end) as amount
from ethereum.beacon_chain.ez_withdrawals
where cast(withdrawal_address as varchar) in (select to_ from eigen_address)
and block_timestamp>=date('2023-06-09')
group by 1
)a
group by 1),

price as (
select date_trunc('day',HOUR) as day, avg(price) as price
from ethereum.price.ez_prices_hourly
where token_address is null
and symbol = 'ETH'
and HOUR >= date('2023-01-01')
group by 1
)

select a.day,netflow, amount, amount* price as usd_tvl from (
select a.day, coalesce(netflow,0) as netflow, sum(coalesce(netflow,0)) over (order by a.day) as amount
from query_3225625 a
left join eigen_balance_eth b 
on a.day=b.day)a
left join price b
on a.day=b.day
where amount>0
and a.day<=date(now())



Query-3225625


with days as (
  select 
    dateadd(day, seq4(), '2008-01-01') as day
  from table(generator(rowcount => 10000)) -- generates 10,000 days
  where dateadd(day, seq4(), '2008-01-01') <= '2035-01-01'

  union all

  select 
    dateadd(day, seq4(), '2035-01-02') as day
  from table(generator(rowcount => 10000)) 
  where dateadd(day, seq4(), '2035-01-02') <= '2062-01-01'
)

select day from days
order by day;



with
lst_tokens as (
    select * from (values
            ('0x57ba429517c3473b6d34ca9acd56c0e735b94c02', 'osETH', '0xf1c9acdc66974dfb6decb12aa385b9cd01190e38'),
            ('0x1bee69b7dfffa4e2d53c2a2df135c388ad25dcd2', 'rETH', '0xae78736cd615f374d3085123a210448e74fc6393'),
            ('0xa4c637e0f704745d182e4d38cab7e7485321d059', 'oETH', '0x856c4efb76c1d1ae02e20ceb03a2a6a08b0b8dc3'),
            ('0x9d7ed45ee2e8fc5482fa2428f15c971e6369011d', 'ETHx', '0xa35b1b31ce002fbf2058d22f30f95d405200a15b'),
            ('0x54945180db7943c0ed0fee7edab2bd24620256bc', 'cbETH', '0xbe9895146f7af43049ca1c1ae358b0541ea49704'),
            ('0x93c4b944d05dfe6df7645a86cd2206016c51564d', 'stETH', '0xae7ab96520de3a18e5e111b5eaab095312d7fe84'),
            ('0x0fe4f44bee93503346a3ac9ee5a26b130a5796d6', 'swETH', '0xf951e335afb289353dc249e82926178eac7ded78'),
            ('0x13760f50a9d7377e4f20cb8cf9e4c26586c658ff', 'ankrETH', '0xe95a203b1a91a908f9b9ce46459d101078c2c3cb'),
            ('0x7ca911e83dabf90c90dd3de5411a10f1a6112184', 'wBETH', '0xa2e3356610840701bdf5611a53974510ae27e2e1'),
            ('0x8ca7a5d6f3acd3a7a8bc468a8cd0fb14b6bd28b6', 'sfrxETH', '0xac3e018457b222d93114458476f3e3416abbe38f'),
            ('0x298afb19a105d59e74658c4c334ff360bade6dd2', 'mETH', '0xd5f7838f5c461feff7fe49ea5ebaf7728bb0adfa'),
            ('0xae60d8180437b5c34bb956822ac2710972584473', 'LsETH', '0x8c1bed5b9a0928467c9b1341da1d7bd5e10b6549')
    ) as t(restaking_contract, symbol, token_contract)
),

tmp as (
select
from_address as addr,
et.symbol,
cast(raw_amount as double) / POW(10, decimals) as amount
from ethereum.core.ez_token_transfers et
inner join lst_tokens lst on lower(et.to_address) = lower(lst.restaking_contract) and lower(et.contract_address) = lower(lst.token_contract)
UNION ALL
select
to_address as addr,
et.symbol,
-cast(raw_amount as double) / POW(10, decimals) as amount
from ethereum.core.ez_token_transfers et
inner join lst_tokens lst on lower(et.from_address) = lower(lst.restaking_contract) and lower(et.contract_address) = lower(lst.token_contract)
),
users as (
select
addr,
symbol,
sum(amount) as amount
from tmp
group by 1, 2
having sum(amount) > 0
)
select
sum(amount) as total_lst_eth,
count(distinct addr) as total_depositor,
from users



--ref Dune query---https://dune.com/queries/3295432/5517482
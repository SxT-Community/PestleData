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

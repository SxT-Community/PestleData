-- SELECT
--   tx_hash,
--   COUNT(*) AS occurrences
-- FROM avalanche.core.ez_decoded_event_logs
-- WHERE lower(contract_address) = lower('0xbd3fa81b58ba92a82136038b25adec7066af3155')
--   AND event_name = 'DepositForBurn'
--   -- AND block_timestamp   > '2025-04-01'
-- GROUP BY 1
-- HAVING COUNT(*) > 1;

-- select * from arbitrum.core.ez_token_transfers where lower(from_address) = lower('0x797264c07D86Ef41C96899Ad827b41321eF432B4') and lower(contract_address) = lower('0xaf88d065e77c8cc2239327c5edb3a432268e5831')

-- SELECT
-- *
--   FROM arbitrum.core.ez_decoded_event_logs
--   WHERE lower(origin_from_address) = lower('0x797264c07D86Ef41C96899Ad827b41321eF432B4')
--     AND event_name       = 'DepositForBurn'
--     AND block_timestamp > '2025-04-01';

/* =======================================================================
   0.  Constant: Balancer V2 Vault
   ==================================================================== */
WITH balance_events AS (

    /* -------------------------------------------------------------- */
    /* 0‑A  PoolBalanceChanged  (joins & exits)                        */
    /* -------------------------------------------------------------- */
    SELECT
        DATE_TRUNC('day', e.block_timestamp)                 AS block_timestamp ,
        e.contract_address                                   ,
        e.decoded_log:"poolId"::STRING                       AS pool_id        ,
        tok.value::STRING                                    AS token_address  ,
        del.value::NUMBER                                    AS delta
    FROM   ethereum.core.ez_decoded_event_logs     e
           , LATERAL FLATTEN ( input => e.decoded_log:"tokens"  ) tok
           , LATERAL FLATTEN ( input => e.decoded_log:"deltas"  ) del
    WHERE  tok.index = del.index
      AND  LOWER(e.contract_address) = LOWER('0xba12222222228d8ba445958a75a0704d566bf2c8')
      AND  e.event_name = 'PoolBalanceChanged'

    UNION ALL

    /* -------------------------------------------------------------- */
    /* 0‑B  Swap – tokenIn  (+amountIn)                               */
    /* -------------------------------------------------------------- */
    SELECT
        DATE_TRUNC('day', e.block_timestamp)                 ,
        e.contract_address                                   ,
        e.decoded_log:"poolId"::STRING                       ,
        LOWER(e.decoded_log:"tokenIn"::STRING)               ,
        e.decoded_log:"amountIn"::NUMBER        AS delta
    FROM   ethereum.core.ez_decoded_event_logs e
    WHERE  LOWER(e.contract_address) = LOWER('0xba12222222228d8ba445958a75a0704d566bf2c8')
      AND  e.event_name = 'Swap'

    UNION ALL

    /* -------------------------------------------------------------- */
    /* 0‑C  Swap – tokenOut (‑amountOut)                              */
    /* -------------------------------------------------------------- */
    SELECT
        DATE_TRUNC('day', e.block_timestamp)                 ,
        e.contract_address                                   ,
        e.decoded_log:"poolId"::STRING                       ,
        LOWER(e.decoded_log:"tokenOut"::STRING)              ,
        -1 * e.decoded_log:"amountOut"::NUMBER  AS delta
    FROM   ethereum.core.ez_decoded_event_logs e
    WHERE  LOWER(e.contract_address) = LOWER('0xba12222222228d8ba445958a75a0704d566bf2c8')
      AND  e.event_name = 'Swap'

    UNION ALL

    /* -------------------------------------------------------------- */
    /* 0‑D  PoolBalanceManaged – net change = cashDelta + managedDelta*/
    /* -------------------------------------------------------------- */
    SELECT
        DATE_TRUNC('day', e.block_timestamp)                 ,
        e.contract_address                                   ,
        e.decoded_log:"poolId"::STRING                       ,
        asset.value::STRING                                  ,
        (cash.value::NUMBER + mng.value::NUMBER) AS delta
    FROM   ethereum.core.ez_decoded_event_logs  e
           , LATERAL FLATTEN ( input => e.decoded_log:"assets"        ) asset
           , LATERAL FLATTEN ( input => e.decoded_log:"cashDeltas"    ) cash
           , LATERAL FLATTEN ( input => e.decoded_log:"managedDeltas" ) mng
    WHERE  asset.index = cash.index
      AND  cash.index  = mng.index
      AND  LOWER(e.contract_address) = LOWER('0xba12222222228d8ba445958a75a0704d566bf2c8')
      AND  e.event_name = 'PoolBalanceManaged'
),

/* =======================================================================
   1.  Net daily change per pool / token
   ==================================================================== */
daily_balance AS (
    SELECT
        contract_address,
        block_timestamp,        -- already truncated to day
        pool_id,
        token_address,
        SUM(delta) AS balance
    FROM   balance_events
    GROUP  BY contract_address, block_timestamp, pool_id, token_address
),

/* =======================================================================
   2.  Cumulative balance
   ==================================================================== */
pool_balance AS (
    SELECT
        contract_address,
        block_timestamp,
        pool_id,
        token_address,
        balance,
        SUM(balance) OVER (
            PARTITION BY pool_id, token_address
            ORDER BY block_timestamp
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS cumulative_balance
    FROM daily_balance
),

/* =======================================================================
   3.  Pool & token metadata
   ==================================================================== */
pool_data AS (
    SELECT
        decoded_log:"poolId"::STRING      AS pool_id,
        decoded_log:"poolAddress"::STRING AS pool_address
    FROM   ethereum.core.ez_decoded_event_logs
    WHERE  LOWER(contract_address) = LOWER('0xba12222222228d8ba445958a75a0704d566bf2c8')
      AND  event_name = 'PoolRegistered'
),

labels AS (
    SELECT DISTINCT
        LOWER(address) AS address,
        address_name   AS pool_symbol
    FROM ethereum.core.dim_labels
),

token_symbols AS (
    SELECT DISTINCT
        LOWER(contract_address) AS token_address,
        symbol                  AS token_symbol,
        decimals
    FROM ethereum.core.ez_token_transfers
),


prices AS (
    SELECT
        DATE_TRUNC('hour', hour) AS hour,
        LOWER(token_address)     AS token_address,
        price
    FROM ethereum.price.ez_prices_hourly
)

/* =======================================================================
   4.  Final output
   ==================================================================== */
SELECT
    pb.contract_address,
    pb.block_timestamp,
    pb.pool_id,
    pd.pool_address,
    l.pool_symbol,
    pb.token_address,
    ts.token_symbol,
    pb.balance / POW(10, ts.decimals) AS DAILY_CHANGE,
    pb.cumulative_balance / POW(10, ts.decimals) AS cumulative_balance,
    pr.price,
    (pb.cumulative_balance / POW(10, ts.decimals)) * pr.price AS balance_usd
FROM pool_balance pb
LEFT   JOIN pool_data     pd ON pb.pool_id = pd.pool_id
LEFT   JOIN labels        l  ON LOWER(pd.pool_address)  = l.address
LEFT   JOIN token_symbols ts ON LOWER(pb.token_address) = ts.token_address
LEFT   JOIN prices        pr ON LOWER(pb.token_address) = pr.token_address
                              AND DATE_TRUNC('hour', pb.block_timestamp) = pr.hour

ORDER  BY pb.block_timestamp DESC, pb.pool_id, pb.token_address
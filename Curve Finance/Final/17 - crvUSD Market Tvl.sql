WITH add_market_events AS (
    SELECT
        block_timestamp,
        block_number,
        tx_hash,
        decoded_log:"amm"::STRING AS amm_contract,
        decoded_log:"collateral"::STRING AS collateral_contract
    FROM ethereum.core.ez_decoded_event_logs
    WHERE 
        origin_from_address IN (LOWER('0x425d16B0e08a28A3Ff9e4404AE99D78C0a076C5A'), 
                                LOWER('0x7a16fF8270133F063aAb6C9977183D9e72835428'))
        AND event_name = 'AddMarket'
),
token_transfers AS (
    SELECT 
        DATE(tt.block_timestamp) AS transfer_date,
        ame.amm_contract,
        ame.collateral_contract AS token_contract,
        tt.symbol,
        CASE 
            WHEN tt.from_address = ame.amm_contract THEN -tt.AMOUNT  -- Outflows are negative
            WHEN tt.to_address = ame.amm_contract THEN tt.AMOUNT     -- Inflows are positive
            ELSE 0
        END AS net_flow
    FROM ethereum.core.ez_token_transfers tt
    JOIN add_market_events ame
        ON tt.contract_address = ame.collateral_contract
    WHERE 
        tt.from_address = ame.amm_contract
        OR tt.to_address = ame.amm_contract
)
SELECT 
    transfer_date,
    amm_contract,
    token_contract,
    symbol,
    SUM(CASE WHEN net_flow > 0 THEN net_flow ELSE 0 END) AS total_inflow,
    SUM(CASE WHEN net_flow < 0 THEN ABS(net_flow) ELSE 0 END) AS total_outflow,
    SUM(SUM(net_flow)) OVER (
        PARTITION BY amm_contract, token_contract, symbol 
        ORDER BY transfer_date 
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS cumulative_balance
FROM token_transfers
GROUP BY transfer_date, amm_contract, token_contract, symbol
ORDER BY transfer_date, amm_contract, token_contract, symbol;
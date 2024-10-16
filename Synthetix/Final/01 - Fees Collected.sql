WITH eth_fee AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(Amount) AS eth_amount
    FROM 
        ethereum.core.ez_token_transfers 
    WHERE 
        contract_address = LOWER('0x57Ab1ec28D129707052df4dF418D58a2D46d5f51')
        AND to_address = LOWER('0xfeefeefeefeefeefeefeefeefeefeefeefeefeef')
    GROUP BY day
),

opt_fee AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(Amount) AS opt_amount
    FROM 
        optimism.core.ez_token_transfers 
    WHERE 
        contract_address = LOWER('0x8c6f28f2F1A3C87F0f938b96d27520d9751ec8d9') 
        AND to_address = LOWER('0xfeefeefeefeefeefeefeefeefeefeefeefeefeef')
    GROUP BY day
),

base_fee AS (
    SELECT 
        DATE_TRUNC('day', BLOCK_TIMESTAMP) AS day,
        SUM(Amount) AS base_amount
    FROM 
        base.core.ez_token_transfers 
    WHERE 
        contract_address = LOWER('0x09d51516F38980035153a554c26Df3C6f51a23C3') 
        AND (
            to_address = LOWER('0x53f1E640C058337a12D036265681bC172e6fB962')
            OR to_address = LOWER('0x632cAa10A56343C5e6C0c066735840c096291B18')
        )
    GROUP BY day
)

SELECT 
    COALESCE(eth_fee.day, opt_fee.day, base_fee.day) AS day,
    COALESCE(eth_fee.eth_amount, 0) AS eth_susd_fees_collected,
    COALESCE(opt_fee.opt_amount, 0) AS opt_susd_fees_collected,
    COALESCE(base_fee.base_amount, 0) AS base_susd_fees_collected,
    COALESCE(eth_fee.eth_amount, 0) + COALESCE(opt_fee.opt_amount, 0) + COALESCE(base_fee.base_amount, 0) AS total_fee_amount,
    SUM(COALESCE(eth_fee.eth_amount, 0) + COALESCE(opt_fee.opt_amount, 0) + COALESCE(base_fee.base_amount, 0))
    OVER (ORDER BY COALESCE(eth_fee.day, opt_fee.day, base_fee.day)) AS susd_cumulative_fees_collected
FROM 
    eth_fee 
FULL OUTER JOIN 
    opt_fee ON eth_fee.day = opt_fee.day
FULL OUTER JOIN 
    base_fee ON COALESCE(eth_fee.day, opt_fee.day) = base_fee.day
ORDER BY 
    day;

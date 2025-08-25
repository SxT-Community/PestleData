SELECT
  DATE_TRUNC('day', block_timestamp) AS day,

  SUM(
    CASE WHEN LOWER(token_in)  = LOWER('0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b') THEN amount_in  ELSE 0 END +
    CASE WHEN LOWER(token_out) = LOWER('0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b') THEN amount_out ELSE 0 END
  ) AS gfi_volume_token,
  
  SUM(
    CASE WHEN LOWER(token_in)  = LOWER('0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b') THEN amount_in_usd  ELSE 0 END +
    CASE WHEN LOWER(token_out) = LOWER('0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b') THEN amount_out_usd ELSE 0 END
  ) AS gfi_volume_usd
FROM ethereum.defi.ez_dex_swaps
GROUP BY 1
HAVING
  SUM(
    CASE WHEN LOWER(token_in)  = LOWER('0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b') THEN amount_in  ELSE 0 END +
    CASE WHEN LOWER(token_out) = LOWER('0xdab396ccf3d84cf2d07c4454e10c8a6f5b008d2b') THEN amount_out ELSE 0 END
  ) > 0
ORDER BY day desc;
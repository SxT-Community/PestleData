# Balancer Daily Snapshot (Tokens & Fees)

Produce daily snapshots for every Balancer V2 pool on Ethereum:

- Snapshot pool token balances at the first block of each day
- Compute swap fees (gross, protocol share, LP share) per token from Vault Swap logs
- Compute flash-loan fees per token from Vault FlashLoan logs
- (Optional) Emit ProtocolFeesCollector per-token balance deltas across the day (helps infer yield/other protocol fees)

**Outputs:** NDJSON (one JSON per line) or CSV

---

## Requirements

- Python 3.9+
- Packages: `web3`, `hexbytes`, `requests`, `snowflake-connector-python`
- An Ethereum RPC endpoint (must support historical `eth_call` & `eth_getLogs`)
- Snowflake access with:
  - `ETHEREUM_ONCHAIN_CORE_DATA.CORE.fact_blocks`
  - `ETHEREUM_ONCHAIN_CORE_DATA.CORE.ez_decoded_event_logs`

```bash
pip install web3 hexbytes requests snowflake-connector-python
```

## Config

Create `snowflake_config.json`:

```json
{
  "user": "YOUR_USER",
  "password": "YOUR_PASSWORD",
  "organization": "ORGCODE",          // optional if your account is org-scoped
  "account": "ACCTCODE",
  "warehouse": "COMPUTE_WH",
  "database": "ETHEREUM_ONCHAIN_CORE_DATA",
  "schema": "CORE",
  "role": "ACCOUNTADMIN",
  "rpc_url": "https://your-ethereum-node"
}
```

## Usage

```bash
python3 balancer_daily.py \
  --config ./snowflake_config.json \
  --limit-days 2 \
  --humanize \
  --collector-deltas \
  --format csv \
  --out balancer_daily.csv
```

### Flags

- `--config` Path to JSON config file
- `--out` Output file path (default: `balancer_daily.ndjson`)
- `--format` `ndjson` (default) or `csv`
- `--limit-days N` Only the most recent N days per pool (faster runs)
- `--humanize` Include token_decimals and human-unit values
- `--collector-deltas` Emit ProtocolFeesCollector balance deltas per token/day

## What's Emitted

Each record has a `record_type`:

- **`pool_tokens`** — snapshot from `Vault.getPoolTokens(poolId)`
  - `balance_raw` (raw ERC-20 units)
  - `balance_token_units` (if `--humanize`)
- **`swap_fees`** — fees from Vault Swap logs
  - `fee_raw_gross = amountIn * poolSwapFeePct`
  - `fee_raw_protocol = fee_raw_gross * protocolSwapFeePct`
  - `fee_raw_lp = fee_raw_gross - fee_raw_protocol`
- **`flash_fees`** — fees from FlashLoan logs (per token)
- **`protocol_collector_delta`** (optional) — ERC-20 balance delta in the ProtocolFeesCollector wallet across the day

**Common fields:** `day`, `from_block`, `to_block_exclusive`, `pool_address`, `pool_id`, `token`, and percentages in 1e18 precision (e.g., `pool_swap_fee_pct_1e18`).
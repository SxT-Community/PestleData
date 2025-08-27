#!/usr/bin/env python3
import os, json, argparse, time, csv
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime

import snowflake.connector
from web3 import Web3
from hexbytes import HexBytes
from requests.exceptions import ConnectionError as ReqConnectionError, ReadTimeout as ReqReadTimeout
from web3.exceptions import TimeExhausted

# -----------------------------
# Constants / ABIs
# -----------------------------
VAULT_ADDRESS = Web3.to_checksum_address("0xBA12222222228d8Ba445958a75a0704d566BF2C8")

# keccak topics
SWAP_TOPIC  = Web3.keccak(text="Swap(bytes32,address,address,uint256,uint256)").hex()
FLASH_TOPIC = Web3.keccak(text="FlashLoan(address,address,uint256,uint256)").hex()

VAULT_ABI = [
    # getPoolTokens(poolId)
    {
        "inputs":[{"internalType":"bytes32","name":"poolId","type":"bytes32"}],
        "name":"getPoolTokens",
        "outputs":[
            {"internalType":"contract IERC20[]","name":"tokens","type":"address[]"},
            {"internalType":"uint256[]","name":"balances","type":"uint256[]"},
            {"internalType":"uint256","name":"lastChangeBlock","type":"uint256"}
        ],
        "stateMutability":"view",
        "type":"function"
    },
    # protocol fees collector address
    {
        "inputs": [],
        "name": "getProtocolFeesCollector",
        "outputs": [{"internalType":"contract IProtocolFeesCollector","name":"","type":"address"}],
        "stateMutability": "view",
        "type": "function"
    },
    # FlashLoan event (ABI present helps some providers)
    {
      "anonymous": False, "inputs": [
        {"indexed": True,  "internalType":"address","name":"recipient","type":"address"},
        {"indexed": True,  "internalType":"address","name":"token","type":"address"},
        {"indexed": False, "internalType":"uint256","name":"amount","type":"uint256"},
        {"indexed": False, "internalType":"uint256","name":"feeAmount","type":"uint256"}
      ],
      "name": "FlashLoan", "type": "event"
    }
]

POOL_FEE_ABI = [
    {"inputs": [], "name": "getSwapFeePercentage",
     "outputs": [{"internalType":"uint256","name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"}
]

PROTOCOL_FEES_COLLECTOR_ABI = [
    {"inputs":[],"name":"getSwapFeePercentage","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[],"name":"getFlashLoanFeePercentage","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"},
    {"inputs":[],"name":"getYieldFeePercentage","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],
     "stateMutability":"view","type":"function"}
]

ERC20_ABI = [
    {"constant":True,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"},
    {"constant":True,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},
    {"constant":True,"inputs":[{"name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"type":"function"}
]

# -----------------------------
# SQL (pools + first blocks)
# -----------------------------
SQL_POOLS_AND_FIRST_BLOCKS = """
WITH first_blocks AS (
  SELECT
    DATE_TRUNC('day', block_timestamp)  AS day,
    MIN(block_number)                   AS first_block_number
  FROM ethereum_onchain_core_data.core.fact_blocks
  GROUP BY 1
),
first_blocks_next AS (
  SELECT
    day,
    first_block_number,
    LEAD(first_block_number) OVER (ORDER BY day) AS next_first_block
  FROM first_blocks
),
pool_registered AS (
  SELECT
    e.block_number,
    e.block_timestamp,
    e.decoded_log:"poolAddress"::string AS pool_address,
    e.decoded_log:"poolId"::string      AS pool_id
  FROM ethereum_onchain_core_data.core.ez_decoded_event_logs e
  WHERE LOWER(e.contract_address) = LOWER('0xBA12222222228d8BA445958a75a0704d566BF2C8')
    AND e.event_name = 'PoolRegistered'
)
SELECT
  fbn.day,
  fbn.first_block_number,
  fbn.next_first_block,
  pr.pool_address,
  pr.pool_id
FROM first_blocks_next fbn
JOIN pool_registered pr
  ON fbn.first_block_number > pr.block_number
ORDER BY pr.pool_address, fbn.day
"""

# -----------------------------
# Config & connections
# -----------------------------
def load_config(path: Optional[str]) -> Dict[str, Any]:
    if path:
        with open(path, "r") as f:
            cfg = json.load(f)
    else:
        cfg = {
            "user": os.getenv("SNOWFLAKE_USER"),
            "password": os.getenv("SNOWFLAKE_PASSWORD"),
            "account": os.getenv("SNOWFLAKE_ACCOUNT"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": os.getenv("SNOWFLAKE_DATABASE", "ETHEREUM_ONCHAIN_CORE_DATA"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA", "CORE"),
            "role": os.getenv("SNOWFLAKE_ROLE"),
            "organization": os.getenv("SNOWFLAKE_ORG"),
            "rpc_url": os.getenv("ETH_RPC_URL")
        }
    for k in ("user","password","account"):
        if not cfg.get(k):
            raise ValueError(f"Missing Snowflake config key: {k}")
    if not cfg.get("rpc_url"):
        raise ValueError("Missing Ethereum RPC URL (add 'rpc_url' in config or ETH_RPC_URL env).")
    return cfg

def connect_snowflake(cfg: Dict[str, Any]) -> snowflake.connector.SnowflakeConnection:
    account = cfg["account"]
    if cfg.get("organization"):
        account = f"{cfg['organization']}-{cfg['account']}"
    conn = snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=account,
        warehouse=cfg.get("warehouse","COMPUTE_WH"),
        database=cfg.get("database","ETHEREUM_ONCHAIN_CORE_DATA"),
        schema=cfg.get("schema","CORE"),
        role=cfg.get("role"),
        client_session_keep_alive=True,
        autocommit=True
    )
    return conn

def connect_web3(rpc_url: str) -> Web3:
    w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 120}))
    if not w3.is_connected():
        raise RuntimeError("Web3 provider not connected. Check rpc_url.")
    return w3

# -----------------------------
# Retry helpers
# -----------------------------
def _with_retries(fn, *args, retries=5, backoff=1.6, first_sleep=0.8, **kwargs):
    attempt = 0
    sleep = first_sleep
    last_err = None
    while attempt < retries:
        try:
            return fn(*args, **kwargs)
        except (ReqConnectionError, ReqReadTimeout, TimeExhausted, ValueError, Exception) as e:
            last_err = e
            time.sleep(sleep)
            sleep *= backoff
            attempt += 1
    raise last_err

# -----------------------------
# Snowflake query
# -----------------------------
def fetch_pool_blocks(conn, limit_days: Optional[int]=None) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    try:
        base = SQL_POOLS_AND_FIRST_BLOCKS.strip().rstrip(';')
        if limit_days:
            sql = f"""
            SELECT * FROM ({base}) q
            WHERE q.next_first_block IS NOT NULL
            QUALIFY ROW_NUMBER() OVER (PARTITION BY pool_address ORDER BY day DESC) <= {limit_days}
            ORDER BY pool_address, day
            """
        else:
            sql = f"SELECT * FROM ({base}) q WHERE q.next_first_block IS NOT NULL ORDER BY pool_address, day"
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]
    finally:
        cur.close()

# -----------------------------
# Hex/topic helpers & unit math
# -----------------------------
def _to_hex_str(v) -> str:
    if isinstance(v, (HexBytes, bytes, bytearray)):
        return Web3.to_hex(v)
    if isinstance(v, str):
        return v if v.startswith("0x") else "0x"+v
    return "0x"+str(v)

def _topic_to_addr(topic) -> str:
    s = _to_hex_str(topic)
    return Web3.to_checksum_address("0x" + s[-40:])

def token_units_from_raw(raw: int, decimals: Optional[int]) -> Optional[float]:
    return None if decimals is None else float(raw) / (10 ** int(decimals))

# -----------------------------
# Web3 helpers & caches
# -----------------------------
_decimals_cache: Dict[str, Optional[int]] = {}
_fee_cache: Dict[Tuple[str,int], int] = {}
_protocol_cache: Dict[Tuple[str,int], Any] = {}  # ("collector", block)->addr, ("pcts", block)->dict

def safe_decimals(w3: Web3, token: str) -> Optional[int]:
    token_l = token.lower()
    if token_l in _decimals_cache:
        return _decimals_cache[token_l]
    try:
        c = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
        d = int(_with_retries(c.functions.decimals().call))
        _decimals_cache[token_l] = d
        return d
    except Exception:
        _decimals_cache[token_l] = None
        return None

def get_pool_tokens_at_block(w3: Web3, pool_id_hex: str, block_number: int) -> Dict[str, Any]:
    vault = w3.eth.contract(address=VAULT_ADDRESS, abi=VAULT_ABI)
    pool_id_bytes32 = Web3.to_bytes(hexstr=pool_id_hex)
    tokens, balances, last_change_block = _with_retries(
        vault.functions.getPoolTokens(pool_id_bytes32).call,
        block_identifier=block_number
    )
    return {
        "tokens": [Web3.to_checksum_address(t) for t in tokens],
        "balances_raw": [int(b) for b in balances],  # raw ERC20 units (token decimals)
        "last_change_block": int(last_change_block),
    }

def get_swap_fee_pct_pool_1e18(w3: Web3, pool_address: str, block_number: int) -> int:
    key = (pool_address.lower(), block_number)
    if key in _fee_cache:
        return _fee_cache[key]
    pool = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=POOL_FEE_ABI)
    pct = int(_with_retries(pool.functions.getSwapFeePercentage().call, block_identifier=block_number))
    _fee_cache[key] = pct
    return pct

def get_protocol_fees_collector_addr(w3: Web3, block_number: int) -> str:
    key = ("collector", block_number)
    if key in _protocol_cache:
        return _protocol_cache[key]
    vault = w3.eth.contract(address=VAULT_ADDRESS, abi=VAULT_ABI)
    addr = _with_retries(vault.functions.getProtocolFeesCollector().call, block_identifier=block_number)
    addr = Web3.to_checksum_address(addr)
    _protocol_cache[key] = addr
    return addr

def get_protocol_fee_pcts(w3: Web3, block_number: int) -> Dict[str,int]:
    key = ("pcts", block_number)
    if key in _protocol_cache:
        return _protocol_cache[key]
    collector = get_protocol_fees_collector_addr(w3, block_number)
    c = w3.eth.contract(address=collector, abi=PROTOCOL_FEES_COLLECTOR_ABI)
    swap_pct  = int(_with_retries(c.functions.getSwapFeePercentage().call, block_identifier=block_number))
    flash_pct = int(_with_retries(c.functions.getFlashLoanFeePercentage().call, block_identifier=block_number))
    try:
        yield_pct = int(_with_retries(c.functions.getYieldFeePercentage().call, block_identifier=block_number))
    except Exception:
        yield_pct = 0
    val = {"swap": swap_pct, "flash": flash_pct, "yield": yield_pct, "collector": collector}
    _protocol_cache[key] = val
    return val

def erc20_balance_of_at(w3: Web3, token: str, owner: str, block: int) -> int:
    c = w3.eth.contract(address=Web3.to_checksum_address(token), abi=ERC20_ABI)
    return int(_with_retries(c.functions.balanceOf(Web3.to_checksum_address(owner)).call, block_identifier=block))

# -----------------------------
# Logs helpers
# -----------------------------
def _get_logs_once(w3: Web3, q: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _with_retries(w3.eth.get_logs, q)

def chunked_get_logs(w3: Web3, query: Dict[str, Any], max_span: int = 2000) -> List[Dict[str, Any]]:
    f = int(query["fromBlock"]); t = int(query["toBlock"])
    out: List[Dict[str, Any]] = []
    cur_f = f
    while cur_f <= t:
        cur_t = min(cur_f + max_span, t)
        q = dict(query)
        q["fromBlock"] = cur_f
        q["toBlock"]   = cur_t
        logs = _get_logs_once(w3, q)
        out.extend(logs)
        cur_f = cur_t + 1
    return out

# -----------------------------
# Decoders
# -----------------------------
def decode_swap_log(log) -> Dict[str, Any]:
    pool_id  = _to_hex_str(log["topics"][1])      # bytes32
    token_in = _topic_to_addr(log["topics"][2])   # address
    data_hex = _to_hex_str(log["data"])[2:]
    amount_in  = int(data_hex[0:64], 16)          # raw ERC20 units (token decimals)
    amount_out = int(data_hex[64:128], 16)
    return {"pool_id": pool_id, "token_in": token_in,
            "amount_in_raw": amount_in, "amount_out_raw": amount_out}

# -----------------------------
# Daily fee collectors (swap, flash, collector deltas)
# -----------------------------
def collect_swap_fees_for_day(
    w3: Web3,
    pool_id_hex: str,
    pool_address: str,
    start_block: int,
    end_block_exclusive: int,
    humanize: bool
) -> List[Dict[str, Any]]:
    end_inclusive = end_block_exclusive - 1 if end_block_exclusive > start_block else start_block
    pool_swap_pct = get_swap_fee_pct_pool_1e18(w3, pool_address, start_block)
    pcts = get_protocol_fee_pcts(w3, start_block)
    proto_swap_pct = pcts["swap"]

    query = {
        "address": VAULT_ADDRESS,
        "fromBlock": start_block,
        "toBlock": end_inclusive,
        "topics": [SWAP_TOPIC, _to_hex_str(pool_id_hex)]
    }
    logs = chunked_get_logs(w3, query, max_span=2000)

    agg: Dict[str, Dict[str,int]] = {}  # token -> {"gross":.., "protocol":.., "lp":..}
    for lg in logs:
        dec = decode_swap_log(lg)
        token = dec["token_in"]
        amt_in = dec["amount_in_raw"]  # raw token decimals
        gross  = (amt_in * pool_swap_pct) // 10**18
        proto  = (gross  * proto_swap_pct) // 10**18
        lp     = gross - proto
        s = agg.setdefault(token, {"gross":0, "protocol":0, "lp":0})
        s["gross"]    += int(gross)
        s["protocol"] += int(proto)
        s["lp"]       += int(lp)

    rows = []
    for token, s in agg.items():
        rec = {
            "record_type": "swap_fees",
            "from_block": start_block,
            "to_block_exclusive": end_block_exclusive,
            "pool_address": Web3.to_checksum_address(pool_address),
            "pool_id": pool_id_hex,
            "token": Web3.to_checksum_address(token),
            "pool_swap_fee_pct_1e18": pool_swap_pct,
            "protocol_swap_fee_pct_1e18": proto_swap_pct,
            "fee_raw_gross": s["gross"],
            "fee_raw_protocol": s["protocol"],
            "fee_raw_lp": s["lp"]
        }
        if humanize:
            d = safe_decimals(w3, token)
            rec["token_decimals"] = d
            rec["fee_units_gross"]    = token_units_from_raw(s["gross"], d)
            rec["fee_units_protocol"] = token_units_from_raw(s["protocol"], d)
            rec["fee_units_lp"]       = token_units_from_raw(s["lp"], d)
        rows.append(rec)
    return rows

def collect_flash_fees_for_day(
    w3: Web3,
    start_block: int,
    end_block_exclusive: int,
    humanize: bool
) -> List[Dict[str, Any]]:
    end_inclusive = end_block_exclusive - 1 if end_block_exclusive > start_block else start_block
    pcts = get_protocol_fee_pcts(w3, start_block)
    proto_flash_pct = pcts["flash"]

    logs = chunked_get_logs(w3, {
        "address": VAULT_ADDRESS,
        "fromBlock": start_block,
        "toBlock": end_inclusive,
        "topics": [FLASH_TOPIC]
    }, max_span=2000)

    out: Dict[str,int] = {}
    for lg in logs:
        token = _topic_to_addr(lg["topics"][2])
        data_hex = _to_hex_str(lg["data"])[2:]
        amount   = int(data_hex[0:64], 16)
        fee_amt  = int(data_hex[64:128], 16)
        if fee_amt == 0:
            fee_amt = (amount * proto_flash_pct) // 10**18
        out[token] = out.get(token, 0) + int(fee_amt)

    rows = []
    for token, fee_raw in out.items():
        rec = {
            "record_type": "flash_fees",
            "from_block": start_block,
            "to_block_exclusive": end_block_exclusive,
            "token": Web3.to_checksum_address(token),
            "protocol_flash_fee_pct_1e18": proto_flash_pct,
            "fee_raw": fee_raw
        }
        if humanize:
            d = safe_decimals(w3, token)
            rec["token_decimals"] = d
            rec["fee_token_units"] = token_units_from_raw(fee_raw, d)
        rows.append(rec)
    return rows

def collect_protocol_collector_deltas(
    w3: Web3,
    tokens: List[str],
    start_block: int,
    end_block_exclusive: int,
    humanize: bool
) -> List[Dict[str, Any]]:
    end_block = end_block_exclusive - 1 if end_block_exclusive > start_block else start_block
    collector = get_protocol_fees_collector_addr(w3, start_block)
    rows = []
    for token in sorted(set([Web3.to_checksum_address(t) for t in tokens])):
        try:
            b0 = erc20_balance_of_at(w3, token, collector, start_block)
            b1 = erc20_balance_of_at(w3, token, collector, end_block)
            delta = int(b1) - int(b0)
            rec = {
                "record_type": "protocol_collector_delta",
                "collector": collector,
                "from_block": start_block,
                "to_block_inclusive": end_block,
                "token": token,
                "delta_raw": delta
            }
            if humanize:
                d = safe_decimals(w3, token)
                rec["token_decimals"] = d
                rec["delta_token_units"] = token_units_from_raw(delta, d)
            rows.append(rec)
        except Exception as e:
            rows.append({
                "record_type":"protocol_collector_delta",
                "collector": collector,
                "token": token,
                "from_block": start_block, "to_block_inclusive": end_block,
                "error": f"{type(e).__name__}: {e}"
            })
    return rows

# -----------------------------
# Output helpers (NDJSON or CSV)
# -----------------------------
def write_output(records: List[Dict[str, Any]], out_path: str, fmt: str) -> int:
    """
    fmt: 'ndjson' or 'csv'
    Returns number of rows written.
    """
    if fmt == "ndjson":
        with open(out_path, "w") as f:
            for rec in records:
                f.write(json.dumps(rec) + "\n")
        return len(records)

    # CSV: union all keys; order with preferred columns first
    preferred = [
        "record_type","day",
        "first_block_number","from_block","to_block_exclusive","to_block_inclusive",
        "pool_address","pool_id","token","collector",
        "token_decimals",
        "balance_raw","balance_token_units",
        "fee_raw_gross","fee_raw_protocol","fee_raw_lp",
        "fee_units_gross","fee_units_protocol","fee_units_lp",
        "protocol_flash_fee_pct_1e18","fee_raw","fee_token_units",
        "delta_raw","delta_token_units",
        "vault_last_change_block",
        "pool_swap_fee_pct_1e18","protocol_swap_fee_pct_1e18",
        "reason","info","error"
    ]
    all_keys: Set[str] = set()
    for r in records:
        all_keys.update(r.keys())
    ordered = [k for k in preferred if k in all_keys] + sorted(all_keys - set(preferred))

    def flat(v):
        if isinstance(v, (str, int, float)) or v is None:
            return v
        return json.dumps(v, separators=(",",":"))

    with open(out_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ordered)
        writer.writeheader()
        for r in records:
            row = {k: flat(r.get(k)) for k in ordered}
            writer.writerow(row)
    return len(records)

# -----------------------------
# Main pipeline
# -----------------------------
def run(cfg_path: Optional[str], out_path: str, limit_days: Optional[int], humanize: bool, collector_deltas: bool, out_format: str):
    cfg = load_config(cfg_path)
    w3  = connect_web3(cfg["rpc_url"])
    conn = connect_snowflake(cfg)

    rows = fetch_pool_blocks(conn, limit_days=limit_days)
    conn.close()
    print(f"Fetched {len(rows)} (pool, day, first_block, next_first_block) rows.")

    processed_day_ranges: Set[Tuple[int,int]] = set()
    day_tokens_seen: Dict[Tuple[int,int], Set[str]] = {}
    out_records: List[Dict[str, Any]] = []

    for idx, r in enumerate(rows, 1):
        day = r["day"]
        day_str = str(day)
        start_block = int(r["first_block_number"])
        end_block_exclusive = int(r["next_first_block"])
        pool_id = r["pool_id"]
        pool_address = Web3.to_checksum_address(r["pool_address"])

        # 1) Pool snapshot
        try:
            snap = get_pool_tokens_at_block(w3, pool_id, start_block)
            for token, raw in zip(snap["tokens"], snap["balances_raw"]):
                rec = {
                    "record_type": "pool_tokens",
                    "day": day_str,
                    "first_block_number": start_block,
                    "pool_address": pool_address,
                    "pool_id": pool_id,
                    "token": token,
                    "balance_raw": int(raw),
                    "vault_last_change_block": snap["last_change_block"]
                }
                if humanize:
                    dec = safe_decimals(w3, token)
                    rec["token_decimals"] = dec
                    rec["balance_token_units"] = token_units_from_raw(int(raw), dec)
                out_records.append(rec)
        except Exception as e:
            out_records.append({
                "record_type": "pool_tokens",
                "day": day_str,
                "first_block_number": start_block,
                "pool_address": pool_address,
                "pool_id": pool_id,
                "error": f"{type(e).__name__}: {e}"
            })

        # 2) Swap fees (per pool)
        try:
            fee_rows = collect_swap_fees_for_day(
                w3=w3,
                pool_id_hex=pool_id,
                pool_address=pool_address,
                start_block=start_block,
                end_block_exclusive=end_block_exclusive,
                humanize=humanize
            )
            for rec in fee_rows:
                rec["day"] = day_str
                out_records.append(rec)
                key = (start_block, end_block_exclusive)
                day_tokens_seen.setdefault(key, set()).add(rec["token"])
        except Exception as e:
            out_records.append({
                "record_type": "swap_fees",
                "day": day_str,
                "from_block": start_block,
                "to_block_exclusive": end_block_exclusive,
                "pool_address": pool_address,
                "pool_id": pool_id,
                "error": f"{type(e).__name__}: {e}"
            })

        # 3) Flash fees (once per day range)
        day_key = (start_block, end_block_exclusive)
        if day_key not in processed_day_ranges:
            try:
                flash_rows = collect_flash_fees_for_day(
                    w3=w3,
                    start_block=start_block,
                    end_block_exclusive=end_block_exclusive,
                    humanize=humanize
                )
                for rec in flash_rows:
                    rec["day"] = day_str
                    out_records.append(rec)
                    day_tokens_seen.setdefault(day_key, set()).add(rec["token"])
            except Exception as e:
                out_records.append({
                    "record_type": "flash_fees",
                    "day": day_str,
                    "from_block": start_block,
                    "to_block_exclusive": end_block_exclusive,
                    "error": f"{type(e).__name__}: {e}"
                })
            processed_day_ranges.add(day_key)

        if idx % 25 == 0:
            print(f"Processed {idx}/{len(rows)} rows…")

    # 4) Protocol collector deltas (optional)
    if collector_deltas:
        for (start_block, end_block_exclusive), tokens in day_tokens_seen.items():
            try:
                rows2 = collect_protocol_collector_deltas(
                    w3=w3,
                    tokens=list(tokens),
                    start_block=start_block,
                    end_block_exclusive=end_block_exclusive,
                    humanize=humanize
                )
                out_records.extend(rows2)
            except Exception as e:
                out_records.append({
                    "record_type": "protocol_collector_delta",
                    "from_block": start_block,
                    "to_block_exclusive": end_block_exclusive,
                    "error": f"{type(e).__name__}: {e}"
                })

    # Write output
    n = write_output(out_records, out_path, out_format.lower())
    print(f"Wrote {n} records to {out_path} ({out_format.upper()})")

# -----------------------------
# CLI
# -----------------------------
if __name__ == "__main__":
    ap = argparse.ArgumentParser(
        description="Balancer daily snapshots: tokens, swap fees, flash fees (+ optional ProtocolFeesCollector deltas)"
    )
    ap.add_argument("--config", "-c", help="Path to JSON config with Snowflake + rpc_url")
    ap.add_argument("--out", default="balancer_daily.ndjson", help="Output file")
    ap.add_argument("--format", choices=["ndjson","csv"], default="ndjson",
                    help="Output format: ndjson (default) or csv")
    ap.add_argument("--limit-days", type=int, default=None,
                    help="Keep only the most recent N days per pool (for quick tests)")
    ap.add_argument("--humanize", action="store_true",
                    help="Also fetch ERC20 decimals and compute token-unit values")
    ap.add_argument("--collector-deltas", action="store_true",
                    help="Also emit ProtocolFeesCollector balance deltas per day")
    args = ap.parse_args()

    run(args.config, args.out, args.limit_days, args.humanize, args.collector_deltas, args.format)
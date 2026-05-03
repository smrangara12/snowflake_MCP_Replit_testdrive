"""
Thin wrapper that launches the Snowflake MCP server with
programmatic_access_token authentication.

Patches two issues in mcp_server_snowflake:
  1. Wrong package name in importlib.metadata.version() lookup
  2. SnowflakeDB uses Snowpark-style .sql() on a plain connector connection
     — replaced with cursor-based execution that works with token auth.
"""
import asyncio
import importlib.metadata
import os
import time
import uuid

# ── Patch 1: wrong package name ───────────────────────────────────────────────
_orig_version = importlib.metadata.version
def _patched_version(name):
    if name == "mcp_snowflake_server":
        name = "mcp_server_snowflake"
    return _orig_version(name)
importlib.metadata.version = _patched_version

# ── Import server (after patch so the version lookup succeeds) ────────────────
from snowflake import connector
from mcp_server_snowflake import server
from mcp_server_snowflake.server import SnowflakeDB

# ── Patch 2: replace Snowpark-style .sql() with plain cursor calls ────────────
def _init_database(self):
    """Connect using standard connector (supports programmatic_access_token)."""
    try:
        cfg = {k: v for k, v in self.connection_config.items() if v not in (None, "")}
        self.session = connector.connect(**cfg)
        cur = self.session.cursor()
        for key in ("database", "schema", "warehouse"):
            val = self.connection_config.get(key, "")
            if val:
                cur.execute(f"USE {key.upper()} {val.upper()}")
        cur.close()
        self.auth_time = time.time()
    except Exception as e:
        raise ValueError(f"Failed to connect to Snowflake database: {e}")

def _serialize(val):
    """Convert non-JSON-serializable Snowflake types to plain Python types."""
    import decimal, datetime
    if isinstance(val, decimal.Decimal):
        return float(val)
    if isinstance(val, (datetime.date, datetime.datetime)):
        return val.isoformat()
    return val

def _execute_query(self, query: str):
    """Execute query with cursor API and return list-of-dicts + UUID."""
    if not self.session or time.time() - self.auth_time > self.AUTH_EXPIRATION_TIME:
        self._init_database()
    cur = self.session.cursor()
    try:
        cur.execute(query)
        cols = [d[0] for d in cur.description] if cur.description else []
        rows = cur.fetchall()
        result_rows = [
            {col: _serialize(val) for col, val in zip(cols, row)}
            for row in rows
        ]
    finally:
        cur.close()
    return result_rows, str(uuid.uuid4())

SnowflakeDB._init_database = _init_database
SnowflakeDB.execute_query   = _execute_query

# ── Credentials ───────────────────────────────────────────────────────────────

def _clean_account(account: str) -> str:
    suffix = ".snowflakecomputing.com"
    if account.lower().endswith(suffix):
        account = account[: -len(suffix)]
    return account

credentials = {
    "account":       _clean_account(os.environ["SNOWFLAKE_ACCOUNT"]),
    "user":          os.environ["SNOWFLAKE_USER"],
    "authenticator": "programmatic_access_token",
    "token":         os.environ["SNOWFLAKE_TOKEN"],
    "database":      os.environ.get("SNOWFLAKE_DATABASE",  "cpt_demo"),
    "schema":        os.environ.get("SNOWFLAKE_SCHEMA",    "medical"),
    "warehouse":     os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "role":          os.environ.get("SNOWFLAKE_ROLE",      ""),
}

asyncio.run(server.main(
    allow_write=False,
    credentials=credentials,
    prefetch=False,
    log_level="ERROR",
))

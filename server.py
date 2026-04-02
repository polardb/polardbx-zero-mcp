"""
PolarDB-X Cloud Zero MCP Server

An MCP server that gives any AI agent persistent MySQL databases
via PolarDB-X Cloud Zero.  Supports multiple simultaneous instances.

Uses MySQL protocol for SQL execution (pymysql).

Usage:
    polardbx-zero-mcp                    # stdio transport (for Qoder / Claude Desktop)
    polardbx-zero-mcp --transport http   # HTTP transport (for web clients)

Environment variables (all optional):
    POLARDBX_ZERO_API  - Zero API endpoint (default: https://zero.polardbx.com/api/v1/instances)
    MYSQL_URL          - mysql://user:password@host:port/database (registered as instance "env")
    MYSQL_HOST         - MySQL host (registered as instance "env")
    MYSQL_PORT         - MySQL port (default: 3306)
    MYSQL_USERNAME     - MySQL user
    MYSQL_PASSWORD     - MySQL password
    MYSQL_DATABASE     - Database name
"""

import asyncio
import json
import os
import re
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import unquote, urlparse

import httpx
import pymysql
from mcp.server.fastmcp import FastMCP

# --- Constants ---
DEFAULT_ZERO_API = "https://zero.polardbx.com/api/v1/instances"
INSTANCES_FILE = Path.home() / ".polardbx-zero-mcp" / "instances.json"
LEGACY_STATE_FILE = Path.home() / ".polardbx-zero-mcp" / "instance.json"
DEFAULT_TTL_MINUTES = 720  # 12 hours
MAX_CONNECTIONS = 20


# --- Configuration ---

@dataclass
class PolarDBXConfig:
    host: str = ""
    port: int = 3306
    username: str = ""
    password: str = ""
    database: str = ""
    edition: str = ""
    expires_at: str = ""
    assignment_id: str = ""

    @property
    def is_configured(self) -> bool:
        return bool(self.host and self.username and self.password)

    @property
    def is_expired(self) -> bool:
        if not self.expires_at:
            return False
        try:
            exp = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
            return datetime.now(timezone.utc) >= exp
        except Exception:
            return False

    @property
    def remaining_str(self) -> str:
        """Human-readable remaining time, e.g. '5h 32m'."""
        if not self.expires_at:
            return "no expiry"
        try:
            exp = datetime.fromisoformat(self.expires_at.replace("Z", "+00:00"))
            remaining = exp - datetime.now(timezone.utc)
            total_seconds = int(remaining.total_seconds())
            if total_seconds <= 0:
                return "expired"
            hours, remainder = divmod(total_seconds, 3600)
            minutes, _ = divmod(remainder, 60)
            return f"{hours}h {minutes}m"
        except Exception:
            return self.expires_at

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "database": self.database,
            "edition": self.edition,
            "expires_at": self.expires_at,
            "assignment_id": self.assignment_id,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "PolarDBXConfig":
        return cls(
            host=d.get("host", ""),
            port=d.get("port", 3306),
            username=d.get("username", ""),
            password=d.get("password", ""),
            database=d.get("database", ""),
            edition=d.get("edition", ""),
            expires_at=d.get("expires_at", ""),
            assignment_id=d.get("assignment_id", ""),
        )

    @classmethod
    def from_env(cls) -> "PolarDBXConfig":
        """Load config from environment variables."""
        url = os.environ.get("MYSQL_URL", "")
        if url:
            parsed = urlparse(url)
            return cls(
                host=parsed.hostname or "",
                port=parsed.port or 3306,
                username=unquote(parsed.username or ""),
                password=unquote(parsed.password or ""),
                database=unquote(parsed.path.lstrip("/")) or "",
            )
        host = os.environ.get("MYSQL_HOST", "")
        if host:
            return cls(
                host=host,
                port=int(os.environ.get("MYSQL_PORT", "3306")),
                username=os.environ.get("MYSQL_USERNAME", ""),
                password=os.environ.get("MYSQL_PASSWORD", ""),
                database=os.environ.get("MYSQL_DATABASE", ""),
            )
        return cls()


# --- Instance Registry ---

_instances: dict[str, PolarDBXConfig] = {}
_instance_lock = threading.Lock()
_instance_counter: int = 0


def _save_instances():
    """Persist all instances and counter to disk."""
    INSTANCES_FILE.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "counter": _instance_counter,
        "instances": {
            k: v.to_dict() for k, v in _instances.items() if v is not None
        },
    }
    INSTANCES_FILE.write_text(json.dumps(data, indent=2))


def _load_instances():
    """Load instances from disk + env vars + legacy migration at startup."""
    global _instance_counter

    # 1. Try instances.json
    if INSTANCES_FILE.exists():
        try:
            data = json.loads(INSTANCES_FILE.read_text())
            _instance_counter = data.get("counter", 0)
            for iid, cfg_dict in data.get("instances", {}).items():
                config = PolarDBXConfig.from_dict(cfg_dict)
                if config.is_configured and not config.is_expired:
                    _instances[iid] = config
        except Exception as e:
            print(
                f"Warning: Failed to load {INSTANCES_FILE}: {e}",
                file=sys.stderr,
            )
    elif LEGACY_STATE_FILE.exists():
        # 2. Migrate legacy instance.json
        try:
            data = json.loads(LEGACY_STATE_FILE.read_text())
            config = PolarDBXConfig.from_dict(data)
            if config.is_configured and not config.is_expired:
                _instance_counter = 1
                _instances["inst_1"] = config
                _save_instances()
                LEGACY_STATE_FILE.rename(
                    LEGACY_STATE_FILE.with_suffix(".json.migrated")
                )
        except Exception as e:
            print(
                f"Warning: Failed to migrate {LEGACY_STATE_FILE}: {e}",
                file=sys.stderr,
            )

    # 3. Register env-var instance (overwrite "env" each startup)
    env_config = PolarDBXConfig.from_env()
    if env_config.is_configured:
        _instances["env"] = env_config


def _release_connections_for_instance(instance_id: str):
    """Close and remove all connections belonging to an instance."""
    to_close: list[ManagedConnection] = []
    with _conn_lock:
        for cid, mc in list(_connections.items()):
            if mc.instance_id == instance_id:
                to_close.append(mc)
                del _connections[cid]
    for mc in to_close:
        try:
            mc.connection.close()
        except Exception:
            pass


def get_config(instance_id: str) -> PolarDBXConfig:
    """Look up an instance by ID. Raises if not found or expired."""
    with _instance_lock:
        config = _instances.get(instance_id)

    if config is None:
        raise Exception(
            f"Instance '{instance_id}' not found or still being created. "
            "Use list_instances() to see available instances, "
            "or create_instance() to create one."
        )

    if config.is_expired:
        _release_connections_for_instance(instance_id)
        with _instance_lock:
            _instances.pop(instance_id, None)
            _save_instances()
        raise Exception(
            f"Instance '{instance_id}' has expired. "
            "Create a new one with create_instance()."
        )

    return config


async def _create_zero_instance(
    edition: str = "standard",
    ttl_minutes: int = DEFAULT_TTL_MINUTES,
) -> PolarDBXConfig:
    """Create a new PolarDB-X Cloud Zero instance via API."""
    api_url = os.environ.get("POLARDBX_ZERO_API", DEFAULT_ZERO_API)

    body: dict = {}
    if edition:
        body["edition"] = edition
    body["ttlMinutes"] = ttl_minutes

    async with httpx.AsyncClient() as client:
        resp = await client.post(api_url, json=body, timeout=60)

    if resp.status_code != 200:
        raise Exception(
            f"Failed to create PolarDB-X Cloud Zero instance: "
            f"{resp.status_code} {resp.text}"
        )

    data = resp.json()
    instance = data.get("instance", data)
    conn = instance.get("connection", {})

    return PolarDBXConfig(
        host=conn.get("host", ""),
        port=conn.get("port", 3306),
        username=conn.get("username", ""),
        password=conn.get("password", ""),
        database=conn.get("database", ""),
        edition=edition,
        expires_at=instance.get("expiresAt", ""),
        assignment_id=instance.get("id", ""),
    )


# --- Stateful Connection Registry ---

@dataclass
class ManagedConnection:
    """A stateful connection managed by the connection registry."""
    conn_id: str
    instance_id: str
    connection: pymysql.Connection
    database: str
    created_at: float       # time.monotonic()
    last_active: float      # time.monotonic()
    idle_timeout: float     # seconds


_connections: dict[str, ManagedConnection] = {}
_conn_lock = threading.Lock()
_conn_counter: int = 0


# --- MySQL Client ---

def _get_connection(config: PolarDBXConfig, database: str = "") -> pymysql.Connection:
    """Create a MySQL connection using pymysql."""
    db = database or config.database
    kwargs = dict(
        host=config.host,
        port=config.port,
        user=config.username,
        password=config.password,
        charset="utf8mb4",
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
        cursorclass=pymysql.cursors.DictCursor,
        ssl={"ssl": {}},
    )
    if db:
        kwargs["database"] = db
    return pymysql.connect(**kwargs)


def _cleanup_expired_connections():
    """Remove idle-timed-out connections from the registry."""
    now = time.monotonic()
    to_remove: list[ManagedConnection] = []
    with _conn_lock:
        for mc in _connections.values():
            if now - mc.last_active > mc.idle_timeout:
                to_remove.append(mc)
        for mc in to_remove:
            del _connections[mc.conn_id]
    for mc in to_remove:
        try:
            mc.connection.close()
        except Exception:
            pass


def _get_stateful_conn(conn_id: str, instance_id: str) -> pymysql.Connection:
    """Look up a stateful connection, verify instance match and liveness."""
    _cleanup_expired_connections()

    with _conn_lock:
        mc = _connections.get(conn_id)
    if mc is None:
        raise Exception(
            f"Connection '{conn_id}' not found. "
            "It may have been released or expired due to idle timeout."
        )

    # Cross-instance safety check
    if mc.instance_id != instance_id:
        raise Exception(
            f"Connection '{conn_id}' belongs to instance '{mc.instance_id}', "
            f"but you specified instance '{instance_id}'. "
            f"Use a connection acquired for '{instance_id}', or omit conn_id "
            f"for a stateless query."
        )

    # Ping outside the lock (may be slow)
    try:
        mc.connection.ping(reconnect=False)
    except Exception:
        with _conn_lock:
            _connections.pop(conn_id, None)
        raise Exception(
            f"Connection '{conn_id}' is no longer alive "
            "(MySQL server closed it). Please acquire a new connection."
        )

    with _conn_lock:
        mc.last_active = time.monotonic()

    return mc.connection


async def execute_sql(
    sql: str, instance_id: str, database: str = "", conn_id: str = "",
) -> dict:
    """Execute SQL via MySQL protocol. Returns columns, rows, and metadata."""
    config = get_config(instance_id)

    if conn_id:
        conn = _get_stateful_conn(conn_id, instance_id)

        def _execute_stateful():
            with conn.cursor() as cursor:
                cursor.execute(sql)
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return {
                        "columns": columns,
                        "rows": [list(row.values()) for row in rows],
                        "rows_as_dicts": rows,
                        "rows_affected": cursor.rowcount,
                    }
                else:
                    return {
                        "columns": [],
                        "rows": [],
                        "rows_as_dicts": [],
                        "rows_affected": cursor.rowcount,
                        "last_insert_id": cursor.lastrowid,
                    }

        return await asyncio.to_thread(_execute_stateful)

    def _execute():
        conn = _get_connection(config, database=database)
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql)
                if cursor.description:
                    columns = [desc[0] for desc in cursor.description]
                    rows = cursor.fetchall()
                    return {
                        "columns": columns,
                        "rows": [list(row.values()) for row in rows],
                        "rows_as_dicts": rows,
                        "rows_affected": cursor.rowcount,
                    }
                else:
                    conn.commit()
                    return {
                        "columns": [],
                        "rows": [],
                        "rows_as_dicts": [],
                        "rows_affected": cursor.rowcount,
                        "last_insert_id": cursor.lastrowid,
                    }
        finally:
            conn.close()

    return await asyncio.to_thread(_execute)


# --- Formatting ---

def format_results(result: dict, max_rows: int = 100) -> str:
    """Format query results as a readable table."""
    rows = result.get("rows_as_dicts", [])
    columns = result.get("columns", [])

    if not rows or not columns:
        rows_affected = result.get("rows_affected")
        if rows_affected is not None:
            msg = f"OK. Rows affected: {rows_affected}"
            last_id = result.get("last_insert_id")
            if last_id and last_id != 0:
                msg += f". Last insert ID: {last_id}"
            return msg
        return "No results."

    truncated = len(rows) > max_rows
    rows = rows[:max_rows]

    widths = {col: len(str(col)) for col in columns}
    for row in rows:
        for col in columns:
            widths[col] = max(widths[col], len(str(row.get(col, ""))))

    header = " | ".join(str(col).ljust(widths[col]) for col in columns)
    separator = "-+-".join("-" * widths[col] for col in columns)
    lines = [header, separator]
    for row in rows:
        line = " | ".join(
            str(row.get(col, "")).ljust(widths[col]) for col in columns
        )
        lines.append(line)

    text = "\n".join(lines)
    if truncated:
        text += f"\n... (showing first {max_rows} rows)"
    return text


# --- MCP Server ---

mcp = FastMCP(
    "PolarDB-X Cloud Zero",
    instructions=(
        "You have access to PolarDB-X Cloud Zero MySQL-compatible databases.\n"
        "This server supports MULTIPLE simultaneous instances.\n\n"
        "WORKFLOW:\n"
        "1. Call list_instances() to see available instances.\n"
        "2. If none exist, call create_instance() to create one.\n"
        "3. ALL SQL and connection tools require an explicit instance_id parameter.\n"
        "4. Use execute_sql_tool(sql, instance_id=...) to run queries.\n\n"
        "CONNECTIONS:\n"
        "For session state (SET variables, BEGIN/COMMIT/ROLLBACK), use\n"
        "acquire_connection(instance_id=...) to get a conn_id, then pass both\n"
        "instance_id and conn_id to execute_sql_tool(). Connections are bound\n"
        "to their instance and cannot be used across instances.\n"
        "Remember to call release_connection() when done.\n\n"
        "PolarDB-X is a MySQL-compatible distributed SQL database with support "
        "for partitioning, columnar storage, TTL, and more."
    ),
)


# --- Instance Management Tools ---

@mcp.tool()
async def list_instances() -> str:
    """List all registered PolarDB-X instances with their status.

    Shows instance_id, edition, host, status, and expiry for each instance.
    Call this first to discover available instances before running SQL.
    """
    with _instance_lock:
        items = [
            (iid, cfg) for iid, cfg in _instances.items() if cfg is not None
        ]

    if not items:
        return (
            "No instances registered.\n"
            "Call create_instance() to create a PolarDB-X Cloud Zero instance."
        )

    col_id = max(len("instance_id"), max(len(iid) for iid, _ in items))
    col_ed = max(len("edition"), 10)
    col_host = max(len("host"), max(len(c.host) for _, c in items))

    lines = [
        f"{'instance_id':<{col_id}} | {'edition':<{col_ed}} | {'host':<{col_host}} | status  | expires",
        f"{'-' * col_id}-+-{'-' * col_ed}-+-{'-' * col_host}-+---------+--------",
    ]
    active = 0
    for iid, config in items:
        ed = config.edition or "-"
        if config.is_expired:
            status = "Expired"
        else:
            status = "Active"
            active += 1
        remaining = config.remaining_str
        lines.append(
            f"{iid:<{col_id}} | {ed:<{col_ed}} | {config.host:<{col_host}} | {status:<7} | {remaining}"
        )
    lines.append(f"\nTotal: {len(items)} instances ({active} active)")
    return "\n".join(lines)


@mcp.tool()
async def create_instance(
    name: str = "",
    edition: str = "standard",
    ttl_minutes: int = DEFAULT_TTL_MINUTES,
) -> str:
    """Create a new PolarDB-X Cloud Zero instance.

    Multiple instances can coexist. Each gets a unique instance_id.

    Args:
        name: Optional name for this instance (e.g. "mydb").
              If empty, auto-generates inst_1, inst_2, etc.
              Must match [a-zA-Z0-9_-], max 50 chars.
        edition: Instance edition - "standard" or "enterprise"
        ttl_minutes: Instance TTL in minutes (default: 720 = 12 hours)
    """
    global _instance_counter
    try:
        # Resolve instance_id and reserve the slot under lock
        with _instance_lock:
            if name:
                if not re.match(r'^[a-zA-Z0-9_-]{1,50}$', name):
                    return "Error: name must be 1-50 chars, only letters/digits/_/-"
                instance_id = name
                if instance_id in _instances:
                    return (
                        f"Error: Instance '{instance_id}' already exists. "
                        "Use remove_instance() first, or pick a different name."
                    )
            else:
                while True:
                    _instance_counter += 1
                    instance_id = f"inst_{_instance_counter}"
                    if instance_id not in _instances:
                        break
            # Reserve the slot with a sentinel to prevent concurrent collision
            _instances[instance_id] = None  # type: ignore[assignment]

        try:
            config = await _create_zero_instance(edition=edition, ttl_minutes=ttl_minutes)
        except Exception:
            # Un-reserve on API failure
            with _instance_lock:
                _instances.pop(instance_id, None)
            raise

        with _instance_lock:
            _instances[instance_id] = config
            _save_instances()

        info = (
            f"Instance created successfully!\n"
            f"Instance ID: {instance_id}\n"
            f"Edition: {config.edition}\n"
            f"TTL: {ttl_minutes} minutes\n"
            f"Host: {config.host}\n"
            f"Port: {config.port}\n"
            f"Database: {config.database}\n"
        )
        if config.expires_at:
            info += f"Expires: {config.expires_at}\n"
        if config.assignment_id:
            info += f"Assignment ID: {config.assignment_id}\n"
        return info
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def remove_instance(instance_id: str) -> str:
    """Remove an instance and close all its connections.

    Args:
        instance_id: The instance to remove
    """
    with _instance_lock:
        config = _instances.pop(instance_id, None)
    if config is None:
        return f"Instance '{instance_id}' not found."

    _release_connections_for_instance(instance_id)

    with _instance_lock:
        _save_instances()

    return (
        f"Instance '{instance_id}' removed.\n"
        f"All connections for this instance have been closed."
    )


@mcp.tool()
async def get_instance_status(instance_id: str) -> str:
    """Check a specific PolarDB-X instance's status.

    Args:
        instance_id: The instance to check
    """
    with _instance_lock:
        config = _instances.get(instance_id)

    if config is None:
        return (
            f"Instance '{instance_id}' not found.\n"
            "Use list_instances() to see available instances."
        )

    if config.is_expired:
        return (
            f"Status: Expired\n"
            f"Instance ID: {instance_id}\n"
            f"Expired at: {config.expires_at}\n"
            "Call create_instance() to create a new one."
        )

    info = (
        f"Status: Active\n"
        f"Instance ID: {instance_id}\n"
        f"Edition: {config.edition or '-'}\n"
        f"Host: {config.host}\n"
        f"Port: {config.port}\n"
        f"Database: {config.database}\n"
    )
    if config.assignment_id:
        info += f"Assignment ID: {config.assignment_id}\n"
    if config.expires_at:
        info += f"Expires at: {config.expires_at}\n"
        info += f"Remaining: {config.remaining_str}\n"
    else:
        info += "Expires at: no expiry (user-provided instance)\n"
    return info


# --- SQL Tools ---

@mcp.tool()
async def execute_sql_tool(
    sql: str, instance_id: str, database: str = "", conn_id: str = "",
) -> str:
    """Execute any SQL statement on a PolarDB-X instance.

    Supports all SQL types: SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP,
    SHOW, DESCRIBE, EXPLAIN, and any other valid MySQL statement.

    Args:
        sql: The SQL statement to execute
        instance_id: Target instance (from create_instance or list_instances)
        database: Target database name (optional)
        conn_id: Stateful connection ID from acquire_connection() (optional).
                 When provided, SQL runs on that persistent connection with no
                 auto-commit. When omitted, uses a fresh one-shot connection.
    """
    try:
        result = await execute_sql(
            sql, instance_id=instance_id, database=database, conn_id=conn_id,
        )
        return format_results(result)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def batch_execute(
    statements: list[str], instance_id: str, database: str = "", conn_id: str = "",
) -> str:
    """Execute multiple SQL statements sequentially on a PolarDB-X instance.

    Args:
        statements: List of SQL statements to execute in order
        instance_id: Target instance (from create_instance or list_instances)
        database: Target database name (optional)
        conn_id: Stateful connection ID (optional)
    """
    results = []
    for i, sql in enumerate(statements):
        try:
            result = await execute_sql(
                sql, instance_id=instance_id, database=database, conn_id=conn_id,
            )
            msg = f"[{i + 1}] OK"
            rows_affected = result.get("rows_affected")
            if rows_affected is not None:
                msg += f" ({rows_affected} rows)"
            results.append(msg)
        except Exception as e:
            results.append(f"[{i + 1}] Error: {e}")
    return "\n".join(results)


@mcp.tool()
async def list_tables(instance_id: str, database: str = "") -> str:
    """List all tables in the database.

    Args:
        instance_id: Target instance
        database: Target database name (optional)
    """
    try:
        result = await execute_sql(
            "SHOW TABLES", instance_id=instance_id, database=database,
        )
        rows = result.get("rows_as_dicts", [])
        if not rows:
            return "No tables found. Use execute_sql_tool() to create one!"

        tables = [list(row.values())[0] for row in rows]
        return "\n".join(tables)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def describe_table(table: str, instance_id: str, database: str = "") -> str:
    """Get the schema of a table (columns, types, keys).

    Args:
        table: Table name to describe
        instance_id: Target instance
        database: Target database name (optional)
    """
    try:
        safe_table = table.replace("`", "``")
        result = await execute_sql(
            f"DESCRIBE `{safe_table}`", instance_id=instance_id, database=database,
        )
        return format_results(result)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_database_info(instance_id: str) -> str:
    """Get database connection info, PolarDB-X version, and instance status.

    Args:
        instance_id: Target instance
    """
    try:
        config = get_config(instance_id)

        version_result = await execute_sql(
            "SELECT VERSION() as version", instance_id=instance_id,
        )
        version_rows = version_result.get("rows_as_dicts", [])
        version = list(version_rows[0].values())[0] if version_rows else "unknown"

        db_result = await execute_sql(
            "SELECT DATABASE() as db", instance_id=instance_id,
        )
        db_rows = db_result.get("rows_as_dicts", [])
        current_db = list(db_rows[0].values())[0] if db_rows else None

        dbs_result = await execute_sql("SHOW DATABASES", instance_id=instance_id)
        all_dbs = [list(r.values())[0] for r in dbs_result.get("rows_as_dicts", [])]
        user_dbs = [d for d in all_dbs if d.lower() not in
                    ("information_schema", "mysql", "performance_schema", "sys",
                     "__cdc__", "polardbx", "metadb")]

        table_count = None
        if current_db:
            try:
                tables_result = await execute_sql(
                    "SHOW TABLES", instance_id=instance_id,
                )
                table_count = len(tables_result.get("rows_as_dicts", []))
            except Exception:
                pass

        info = (
            f"Instance: {instance_id}\n"
            f"Edition: {config.edition or '-'}\n"
            f"Current database: {current_db or '(none)'}\n"
            f"PolarDB-X Version: {version}\n"
            f"Host: {config.host}\n"
            f"Port: {config.port}\n"
        )
        if table_count is not None:
            info += f"Tables in {current_db}: {table_count}\n"
        info += f"User databases: {', '.join(user_dbs) if user_dbs else '(none)'}\n"
        info += f"Connection: MySQL protocol (pymysql)\n"

        if config.expires_at:
            info += f"Instance expires: {config.expires_at}\n"
        if config.assignment_id:
            info += f"Assignment ID: {config.assignment_id}\n"
        info += (
            f"\nPolarDB-X Cloud Zero — Free distributed MySQL for AI agents.\n"
            f"Get yours at https://zero.polardbx.com"
        )
        return info
    except Exception as e:
        return f"Error: {e}"


# --- Connection Tools ---

@mcp.tool()
async def acquire_connection(
    instance_id: str,
    database: str = "",
    idle_timeout_minutes: int = 30,
) -> str:
    """Acquire a persistent stateful connection on a specific instance.

    The connection is bound to the given instance and cannot be used
    with other instances. The conn_id includes the instance name
    (e.g. "db1_conn_1") for easy identification.

    Args:
        instance_id: Target instance to connect to
        database: Target database name (optional)
        idle_timeout_minutes: Auto-release after this many idle minutes (default: 30)
    """
    global _conn_counter
    _cleanup_expired_connections()

    with _conn_lock:
        if len(_connections) >= MAX_CONNECTIONS:
            return (
                f"Error: Maximum number of connections ({MAX_CONNECTIONS}) reached. "
                "Please release unused connections first."
            )
        _conn_counter += 1
        conn_id = f"{instance_id}_conn_{_conn_counter}"

    try:
        config = get_config(instance_id)
        conn = await asyncio.to_thread(_get_connection, config, database)
    except Exception as e:
        return f"Error creating connection: {e}"

    now = time.monotonic()
    mc = ManagedConnection(
        conn_id=conn_id,
        instance_id=instance_id,
        connection=conn,
        database=database,
        created_at=now,
        last_active=now,
        idle_timeout=idle_timeout_minutes * 60,
    )
    with _conn_lock:
        # Re-check limit to prevent concurrent over-allocation
        if len(_connections) >= MAX_CONNECTIONS:
            conn.close()
            return (
                f"Error: Maximum number of connections ({MAX_CONNECTIONS}) reached. "
                "Please release unused connections first."
            )
        _connections[conn_id] = mc

    info = (
        f"Stateful connection acquired.\n"
        f"Connection ID: {conn_id}\n"
        f"Instance: {instance_id}\n"
    )
    if database:
        info += f"Database: {database}\n"
    info += (
        f"Idle timeout: {idle_timeout_minutes} minutes\n"
        f"\nUse this conn_id with execute_sql_tool() or batch_execute() "
        f"to maintain session state and transactions.\n"
        f'Remember to call release_connection("{conn_id}") when done.'
    )
    return info


@mcp.tool()
async def release_connection(conn_id: str) -> str:
    """Release a stateful connection acquired by acquire_connection().

    Closes the connection and removes it from the registry.
    Any uncommitted transaction will be rolled back by the server.

    Args:
        conn_id: The connection ID to release
    """
    with _conn_lock:
        mc = _connections.pop(conn_id, None)
    if mc is None:
        return (
            f"Connection '{conn_id}' not found. "
            "It may have already been released or expired due to idle timeout."
        )
    try:
        await asyncio.to_thread(mc.connection.close)
    except Exception:
        pass
    return f"Connection '{conn_id}' released successfully."


@mcp.tool()
async def release_all_connections(instance_id: str = "") -> str:
    """Release stateful connections.

    Args:
        instance_id: If provided, release only connections for this instance.
                     If empty, release ALL connections across all instances.
    """
    with _conn_lock:
        if instance_id:
            to_release = [
                mc for mc in _connections.values()
                if mc.instance_id == instance_id
            ]
            for mc in to_release:
                del _connections[mc.conn_id]
        else:
            to_release = list(_connections.values())
            _connections.clear()

    if not to_release:
        if instance_id:
            return f"No active connections for instance '{instance_id}'."
        return "No active stateful connections to release."

    for mc in to_release:
        try:
            mc.connection.close()
        except Exception:
            pass
    return f"Released {len(to_release)} connection(s)."


@mcp.tool()
async def list_connections(instance_id: str = "") -> str:
    """List active stateful connections with their status.

    Args:
        instance_id: If provided, show only connections for this instance.
                     If empty, show all connections.
    """
    _cleanup_expired_connections()
    with _conn_lock:
        if instance_id:
            conns = [mc for mc in _connections.values() if mc.instance_id == instance_id]
        else:
            conns = list(_connections.values())

    if not conns:
        if instance_id:
            return f"No active connections for instance '{instance_id}'."
        return "No active stateful connections."

    now = time.monotonic()
    col_cid = max(len("conn_id"), max(len(mc.conn_id) for mc in conns))
    col_inst = max(len("instance"), max(len(mc.instance_id) for mc in conns))

    lines = [
        f"{'conn_id':<{col_cid}} | {'instance':<{col_inst}} | database   | idle_timeout | idle_remaining",
        f"{'-' * col_cid}-+-{'-' * col_inst}-+------------+--------------+---------------",
    ]
    for mc in conns:
        idle_elapsed = now - mc.last_active
        remaining = max(0, mc.idle_timeout - idle_elapsed)
        remaining_m = int(remaining // 60)
        remaining_s = int(remaining % 60)
        timeout_m = int(mc.idle_timeout // 60)
        db_display = mc.database or "(none)"
        lines.append(
            f"{mc.conn_id:<{col_cid}} | {mc.instance_id:<{col_inst}} | {db_display:<10} | {timeout_m:<12}m | {remaining_m}m {remaining_s}s"
        )
    lines.append(f"\nTotal: {len(conns)}/{MAX_CONNECTIONS} connections")
    return "\n".join(lines)


# --- Entry point ---

def main():
    _load_instances()

    transport = "stdio"
    if "--transport" in sys.argv:
        idx = sys.argv.index("--transport")
        if idx + 1 < len(sys.argv):
            transport = sys.argv[idx + 1]

    if transport == "http":
        mcp.run(transport="streamable-http")
    else:
        mcp.run(transport="stdio")


if __name__ == "__main__":
    main()

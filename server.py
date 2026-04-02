"""
PolarDB-X Cloud Zero MCP Server

An MCP server that gives any AI agent a persistent MySQL database
via PolarDB-X Cloud Zero.

Instances are created on demand — call create_instance() to create one,
or provide your own MySQL credentials via environment variables.

Uses MySQL protocol for SQL execution (pymysql).

Usage:
    uv run server.py                    # stdio transport (for Qoder / Claude Desktop)
    uv run server.py --transport http   # HTTP transport (for web clients)

Environment variables (all optional):
    POLARDBX_ZERO_API  - Zero API endpoint (default: https://zero.polardbx.com/api/v1/instances)
    MYSQL_URL          - mysql://user:password@host:port/database (use existing DB)
    MYSQL_HOST         - MySQL host (use existing DB)
    MYSQL_PORT         - MySQL port (default: 3306)
    MYSQL_USERNAME     - MySQL user
    MYSQL_PASSWORD     - MySQL password
    MYSQL_DATABASE     - Database name

If no credentials are provided, call create_instance() to create a PolarDB-X Cloud Zero instance.
"""

import asyncio
import json
import os
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
STATE_FILE = Path.home() / ".polardbx-zero-mcp" / "instance.json"
DEFAULT_TTL_MINUTES = 720  # 12 hours


# --- Configuration ---

@dataclass
class PolarDBXConfig:
    host: str = ""
    port: int = 3306
    username: str = ""
    password: str = ""
    database: str = ""
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

    def to_dict(self) -> dict:
        return {
            "host": self.host,
            "port": self.port,
            "username": self.username,
            "password": self.password,
            "database": self.database,
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

    @classmethod
    def load_saved(cls) -> "PolarDBXConfig | None":
        """Load saved instance from disk."""
        if STATE_FILE.exists():
            try:
                data = json.loads(STATE_FILE.read_text())
                config = cls.from_dict(data)
                if config.is_configured and not config.is_expired:
                    return config
            except Exception:
                pass
        return None

    def save(self):
        """Save instance to disk for reuse."""
        STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        STATE_FILE.write_text(json.dumps(self.to_dict(), indent=2))


# --- Global state ---
_config: PolarDBXConfig | None = None


@dataclass
class ManagedConnection:
    """A stateful connection managed by the connection registry."""
    conn_id: str
    connection: pymysql.Connection
    database: str
    created_at: float       # time.monotonic()
    last_active: float      # time.monotonic()
    idle_timeout: float     # seconds


_connections: dict[str, ManagedConnection] = {}
_conn_lock = threading.Lock()
_conn_counter: int = 0
MAX_CONNECTIONS = 20


async def get_config() -> PolarDBXConfig:
    """Get current PolarDB-X config. Raises if no instance is available."""
    global _config

    if _config and _config.is_configured and not _config.is_expired:
        return _config

    # 1. Try environment variables
    env_config = PolarDBXConfig.from_env()
    if env_config.is_configured:
        _config = env_config
        return _config

    # 2. Try saved instance
    saved = PolarDBXConfig.load_saved()
    if saved:
        _config = saved
        return _config

    # 3. No instance available
    raise Exception(
        "No PolarDB-X instance available. "
        "Please call create_instance() first to create one."
    )


async def _create_zero_instance(
    edition: str = "standard",
    ttl_minutes: int = DEFAULT_TTL_MINUTES,
) -> PolarDBXConfig:
    """Create a new PolarDB-X Cloud Zero instance via API.

    Args:
        edition: Instance edition, "standard" or "enterprise".
        ttl_minutes: Instance TTL in minutes (default: 720 = 12 hours).
    """
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

    config = PolarDBXConfig(
        host=conn.get("host", ""),
        port=conn.get("port", 3306),
        username=conn.get("username", ""),
        password=conn.get("password", ""),
        database=conn.get("database", ""),
        expires_at=instance.get("expiresAt", ""),
        assignment_id=instance.get("id", ""),
    )

    config.save()
    return config


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


def _get_stateful_conn(conn_id: str) -> pymysql.Connection:
    """Look up a stateful connection by ID, verify it's alive, refresh its timer."""
    _cleanup_expired_connections()

    with _conn_lock:
        mc = _connections.get(conn_id)
    if mc is None:
        raise Exception(
            f"Connection '{conn_id}' not found. "
            "It may have been released or expired due to idle timeout."
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


async def execute_sql(sql: str, database: str = "", conn_id: str = "") -> dict:
    """Execute SQL via MySQL protocol. Returns columns, rows, and metadata."""
    config = await get_config()

    if conn_id:
        # --- Stateful path: reuse persistent connection, no auto-commit, no close ---
        conn = _get_stateful_conn(conn_id)

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

    # --- Stateless path: create, auto-commit, close ---
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
        "You have access to a PolarDB-X Cloud Zero MySQL-compatible database.\n"
        "IMPORTANT: Before executing any SQL, you must first have an active instance.\n"
        "Use get_instance_status() to check if an instance is available.\n"
        "If no instance exists, call create_instance() to create one.\n"
        "Then use execute_sql_tool() and other tools to interact with the database.\n"
        "PolarDB-X is a MySQL-compatible distributed SQL database with support "
        "for partitioning, columnar storage, TTL, and more.\n\n"
        "For operations that need session state (SET variables, transactions with "
        "BEGIN/COMMIT/ROLLBACK), use acquire_connection() to get a persistent conn_id, "
        "then pass it to execute_sql_tool(). Remember to call release_connection() when done."
    ),
)


@mcp.tool()
async def get_instance_status() -> str:
    """Check current PolarDB-X instance status.

    Returns whether an instance is available, its connection info,
    and remaining time before expiry.
    Use this to check before running SQL, or to monitor instance lifetime.
    """
    global _config

    # Check in-memory config
    config = _config

    # Try env
    if not config or not config.is_configured:
        env_config = PolarDBXConfig.from_env()
        if env_config.is_configured:
            config = env_config

    # Try saved
    if not config or not config.is_configured:
        config = PolarDBXConfig.load_saved()

    if not config or not config.is_configured:
        return (
            "Status: No instance\n"
            "No PolarDB-X instance is currently available.\n"
            "Call create_instance() to create one."
        )

    if config.is_expired:
        return (
            "Status: Expired\n"
            f"Instance expired at: {config.expires_at}\n"
            "Call create_instance() to create a new one."
        )

    info = (
        f"Status: Active\n"
        f"Host: {config.host}\n"
        f"Port: {config.port}\n"
        f"Database: {config.database}\n"
    )
    if config.assignment_id:
        info += f"Assignment ID: {config.assignment_id}\n"
    if config.expires_at:
        try:
            exp = datetime.fromisoformat(config.expires_at.replace("Z", "+00:00"))
            remaining = exp - datetime.now(timezone.utc)
            total_seconds = int(remaining.total_seconds())
            if total_seconds > 0:
                hours, remainder = divmod(total_seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                info += f"Expires at: {config.expires_at}\n"
                info += f"Remaining: {hours}h {minutes}m\n"
            else:
                info += "Expires at: expired\n"
        except Exception:
            info += f"Expires at: {config.expires_at}\n"
    else:
        info += "Expires at: no expiry (user-provided instance)\n"
    return info


@mcp.tool()
async def create_instance(
    edition: str = "standard",
    ttl_minutes: int = DEFAULT_TTL_MINUTES,
) -> str:
    """Create a new PolarDB-X Cloud Zero instance.

    Drops the current cached instance and creates a new one.
    Use this when you need a fresh database or want a different edition.

    Args:
        edition: Instance edition — "standard" or "enterprise"
        ttl_minutes: Instance TTL in minutes (default: 720 = 12 hours)
    """
    global _config
    try:
        _config = None
        config = await _create_zero_instance(edition=edition, ttl_minutes=ttl_minutes)
        info = (
            f"Instance created successfully!\n"
            f"Edition: {edition}\n"
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
async def acquire_connection(
    database: str = "",
    idle_timeout_minutes: int = 30,
) -> str:
    """Acquire a persistent stateful connection for session variables and transactions.

    Unlike normal SQL execution which uses a fresh connection each time,
    a stateful connection persists across multiple calls, allowing you to:
    - SET session variables (e.g., SET @var = 1)
    - Use transactions (BEGIN, COMMIT, ROLLBACK)
    - Maintain any session-level state

    Returns a conn_id to pass to execute_sql_tool() or batch_execute().
    Remember to call release_connection() when done.

    Args:
        database: Target database name (optional)
        idle_timeout_minutes: Auto-release after this many idle minutes (default: 30)
    """
    global _conn_counter
    _cleanup_expired_connections()

    with _conn_lock:
        if len(_connections) >= MAX_CONNECTIONS:
            return (
                f"Error: Maximum number of stateful connections ({MAX_CONNECTIONS}) reached. "
                "Please release unused connections with release_connection() or "
                "release_all_connections() first."
            )
        _conn_counter += 1
        conn_id = f"conn_{_conn_counter}"

    try:
        config = await get_config()
        conn = await asyncio.to_thread(_get_connection, config, database)
    except Exception as e:
        return f"Error creating connection: {e}"

    now = time.monotonic()
    mc = ManagedConnection(
        conn_id=conn_id,
        connection=conn,
        database=database,
        created_at=now,
        last_active=now,
        idle_timeout=idle_timeout_minutes * 60,
    )
    with _conn_lock:
        _connections[conn_id] = mc

    info = (
        f"Stateful connection acquired.\n"
        f"Connection ID: {conn_id}\n"
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
        conn_id: The connection ID to release (e.g., "conn_1")
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
async def release_all_connections() -> str:
    """Release all stateful connections.

    Closes all persistent connections and clears the registry.
    Use this to clean up when you're done with all stateful operations.
    """
    with _conn_lock:
        all_conns = list(_connections.values())
        _connections.clear()
    if not all_conns:
        return "No active stateful connections to release."
    for mc in all_conns:
        try:
            mc.connection.close()
        except Exception:
            pass
    return f"Released {len(all_conns)} stateful connection(s)."


@mcp.tool()
async def list_connections() -> str:
    """List all active stateful connections with their status.

    Shows conn_id, database, idle timeout, and remaining idle time
    for each connection.
    """
    _cleanup_expired_connections()
    with _conn_lock:
        conns = list(_connections.values())
    if not conns:
        return "No active stateful connections."

    now = time.monotonic()
    lines = ["conn_id   | database   | idle_timeout | idle_remaining",
             "----------+------------+--------------+---------------"]
    for mc in conns:
        idle_elapsed = now - mc.last_active
        remaining = max(0, mc.idle_timeout - idle_elapsed)
        remaining_m = int(remaining // 60)
        remaining_s = int(remaining % 60)
        timeout_m = int(mc.idle_timeout // 60)
        db_display = mc.database or "(none)"
        lines.append(
            f"{mc.conn_id:<9} | {db_display:<10} | {timeout_m:<12}m | {remaining_m}m {remaining_s}s"
        )
    lines.append(f"\nTotal: {len(conns)}/{MAX_CONNECTIONS} connections")
    return "\n".join(lines)


@mcp.tool()
async def execute_sql_tool(sql: str, database: str = "", conn_id: str = "") -> str:
    """Execute any SQL statement on the PolarDB-X database.

    Supports all SQL types: SELECT, INSERT, UPDATE, DELETE, CREATE, ALTER, DROP,
    SHOW, DESCRIBE, EXPLAIN, and any other valid MySQL statement.

    Returns results as a formatted table for queries, or affected row count
    for write operations.

    Args:
        sql: The SQL statement to execute
        database: Target database name (optional, uses instance default if empty)
        conn_id: Stateful connection ID from acquire_connection() (optional).
                 When provided, SQL runs on that persistent connection with no
                 auto-commit. When omitted, uses a fresh one-shot connection.

    Examples:
        execute_sql_tool("SELECT * FROM users LIMIT 10")
        execute_sql_tool("SHOW TABLES", database="my_db")
        execute_sql_tool("SET @x = 1", conn_id="conn_1")
        execute_sql_tool("BEGIN", conn_id="conn_1")
    """
    try:
        result = await execute_sql(sql, database=database, conn_id=conn_id)
        return format_results(result)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def batch_execute(statements: list[str], database: str = "", conn_id: str = "") -> str:
    """Execute multiple SQL statements sequentially.

    Args:
        statements: List of SQL statements to execute in order
        database: Target database name (optional, uses instance default if empty)
        conn_id: Stateful connection ID from acquire_connection() (optional).
                 When provided, all statements run on the same persistent connection.

    Example:
        batch_execute([
            "CREATE TABLE users (id INT PRIMARY KEY AUTO_INCREMENT, name VARCHAR(255))",
            "INSERT INTO users (name) VALUES ('Alice')",
            "INSERT INTO users (name) VALUES ('Bob')"
        ])
    """
    results = []
    for i, sql in enumerate(statements):
        try:
            result = await execute_sql(sql, database=database, conn_id=conn_id)
            msg = f"[{i + 1}] OK"
            rows_affected = result.get("rows_affected")
            if rows_affected is not None:
                msg += f" ({rows_affected} rows)"
            results.append(msg)
        except Exception as e:
            results.append(f"[{i + 1}] Error: {e}")
    return "\n".join(results)


@mcp.tool()
async def list_tables(database: str = "") -> str:
    """List all tables in the database.

    Args:
        database: Target database name (optional, uses instance default if empty)
    """
    try:
        result = await execute_sql("SHOW TABLES", database=database)
        rows = result.get("rows_as_dicts", [])
        if not rows:
            return "No tables found. Use execute_sql_tool() to create one!"

        tables = [list(row.values())[0] for row in rows]
        return "\n".join(tables)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def describe_table(table: str, database: str = "") -> str:
    """Get the schema of a table (columns, types, keys).

    Args:
        table: Table name to describe
        database: Target database name (optional, uses instance default if empty)
    """
    try:
        result = await execute_sql(f"DESCRIBE `{table}`", database=database)
        return format_results(result)
    except Exception as e:
        return f"Error: {e}"


@mcp.tool()
async def get_database_info() -> str:
    """Get database connection info, PolarDB-X version, and instance status."""
    try:
        config = await get_config()

        version_result = await execute_sql("SELECT VERSION() as version")
        version_rows = version_result.get("rows_as_dicts", [])
        version = list(version_rows[0].values())[0] if version_rows else "unknown"

        db_result = await execute_sql("SELECT DATABASE() as db")
        db_rows = db_result.get("rows_as_dicts", [])
        db = list(db_rows[0].values())[0] if db_rows else "unknown"

        tables_result = await execute_sql("SHOW TABLES")
        table_count = len(tables_result.get("rows_as_dicts", []))

        info = (
            f"Database: {db}\n"
            f"PolarDB-X Version: {version}\n"
            f"Host: {config.host}\n"
            f"Port: {config.port}\n"
            f"Tables: {table_count}\n"
            f"Connection: MySQL protocol (pymysql)\n"
        )
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


# --- Entry point ---

def main():
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

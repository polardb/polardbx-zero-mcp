# PolarDB-X Cloud Zero MCP Server

Give any AI agent a persistent MySQL database through the Model Context Protocol.

Create a free PolarDB-X Cloud Zero instance on demand via `create_instance()`, or bring your own MySQL credentials. No signup, no API keys needed.

## How It Works

```
┌──────────────┐       MCP (stdio/http)       ┌──────────────────┐
│   AI Agent   │  ◄──────────────────────────► │  MCP Server      │
│ (Qoder, etc) │                               │  (this project)  │
└──────────────┘                               └────────┬─────────┘
                                                        │
                                     MySQL protocol     │  Zero API (HTTP)
                                     (pymysql + SSL)    │  (on-demand creation)
                                                        │
                                               ┌────────▼─────────┐
                                               │  PolarDB-X Cloud │
                                               │      Zero        │
                                               └──────────────────┘
```

1. Agent calls `create_instance()` (or you provide MySQL credentials via env vars)
2. Server creates a PolarDB-X Cloud Zero instance and caches credentials locally
3. All SQL is executed over the **MySQL wire protocol** (pymysql + SSL)

## Installation

### From PyPI (recommended)

```bash
# Claude Code
claude mcp add polardbx -- uvx polardbx-zero-mcp
```

For other IDEs (Qoder, Cursor, Windsurf, Claude Desktop), add to your MCP config file (mcp.json):

```json
{
  "mcpServers": {
    "polardbx": {
      "command": "uvx",
      "args": ["polardbx-zero-mcp"]
    }
  }
}
```

### From source

```bash
git clone https://github.com/polardb/polardbx-zero-mcp.git
cd polardbx-zero-mcp
uv sync
uv run server.py
```

> HTTP transport: `uvx polardbx-zero-mcp --transport http` (default `http://localhost:8000/mcp`)

## Configuration

By default, no configuration is needed — just call `create_instance()` to get a free database.

To connect to your own MySQL or PolarDB-X instance, pass credentials via `env`:

```json
{
  "mcpServers": {
    "polardbx": {
      "command": "uvx",
      "args": ["polardbx-zero-mcp"],
      "env": {
        "MYSQL_URL": "mysql://user:password@host:3306/mydb"
      }
    }
  }
}
```

Or use individual variables:

| Variable | Description | Default |
|:---|:---|:---|
| `MYSQL_URL` | Full MySQL connection URL | -- |
| `MYSQL_HOST` | MySQL host | -- |
| `MYSQL_PORT` | MySQL port | `3306` |
| `MYSQL_USERNAME` | MySQL user | -- |
| `MYSQL_PASSWORD` | MySQL password | -- |
| `MYSQL_DATABASE` | Database name | -- |

## Tools

### Instance Management

| Tool | Description |
|:---|:---|
| `get_instance_status` | Check if an instance is available, show connection info and remaining TTL |
| `create_instance` | Create a new PolarDB-X Cloud Zero instance |
| `get_database_info` | Database version, connection info, table count |

### SQL Execution

| Tool | Description |
|:---|:---|
| `execute_sql_tool` | Execute any SQL statement (no type restrictions) |
| `batch_execute` | Execute multiple SQL statements sequentially |
| `list_tables` | List all tables in the database |
| `describe_table` | Get table schema (columns, types, keys) |

### Stateful Connections

For operations that need session state (SET variables, transactions):

| Tool | Description |
|:---|:---|
| `acquire_connection` | Create a persistent connection, returns a `conn_id` |
| `release_connection` | Release a specific connection (rolls back uncommitted transactions) |
| `release_all_connections` | Release all connections |
| `list_connections` | List active connections with idle status |

### Key Parameters

**create_instance:**

| Parameter | Type | Default | Description |
|:---|:---|:---|:---|
| `edition` | string | `"standard"` | `"standard"` or `"enterprise"` |
| `ttl_minutes` | int | `720` | Instance TTL in minutes (default 12h) |

**execute_sql_tool:**

| Parameter | Type | Default | Description |
|:---|:---|:---|:---|
| `sql` | string | (required) | SQL statement to execute |
| `database` | string | `""` | Target database (optional) |
| `conn_id` | string | `""` | Stateful connection ID from `acquire_connection()` (optional) |

**batch_execute:**

| Parameter | Type | Default | Description |
|:---|:---|:---|:---|
| `statements` | list[string] | (required) | SQL statements to execute in order |
| `database` | string | `""` | Target database (optional) |
| `conn_id` | string | `""` | Stateful connection ID (optional) |

**acquire_connection:**

| Parameter | Type | Default | Description |
|:---|:---|:---|:---|
| `database` | string | `""` | Target database (optional) |
| `idle_timeout_minutes` | int | `30` | Auto-release after idle (minutes) |

## Development

```bash
uv sync                              # Install dependencies
uv run mcp dev server.py             # Test with MCP Inspector
uv run server.py --transport http    # Run HTTP server
```

## License

Apache-2.0

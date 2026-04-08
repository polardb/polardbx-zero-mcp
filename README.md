# PolarDB-X Cloud Zero MCP Server

Give any AI agent persistent MySQL databases through the Model Context Protocol.

Supports **multiple simultaneous instances** — create, manage, and query several databases at once.

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

1. Agent calls `create_instance()` to create one or more databases (or provide credentials via env vars)
2. Server registers instances in a local registry (`~/.polardbx-zero-mcp/instances.json`)
3. All tools require an explicit `instance_id` — use `list_instances()` to discover them
4. SQL is executed over the **MySQL wire protocol** (pymysql + SSL)

## Installation

```bash
pip install polardbx-zero-mcp
```

### IDE / MCP Client Configuration

**Claude Code:**

```bash
claude mcp add polardbx -- polardbx-zero-mcp
```

**Qoder / Cursor / Windsurf / Claude Desktop** — add to mcp.json:

```json
{
  "mcpServers": {
    "polardbx": {
      "command": "polardbx-zero-mcp"
    }
  }
}
```

### From source

```bash
git clone https://github.com/polardb/polardbx-zero-mcp.git
cd polardbx-zero-mcp
pip install -e .
polardbx-zero-mcp
```

> HTTP transport: `polardbx-zero-mcp --transport http` (default `http://localhost:8000/mcp`)

## Configuration

By default, no configuration is needed — just call `create_instance()` to get a free database.

To connect to your own MySQL or PolarDB-X instance, pass credentials via `env`. They will be registered as `instance_id="env"`:

```json
{
  "mcpServers": {
    "polardbx": {
      "command": "polardbx-zero-mcp",
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
| `list_instances` | List all registered instances with edition, status, and expiry |
| `create_instance` | Create a new PolarDB-X Cloud Zero instance (standard or enterprise) |
| `remove_instance` | Remove an instance and close all its connections |
| `get_instance_status` | Check a specific instance's connection info and remaining TTL |

### SQL Execution

All SQL tools require `instance_id`. Use `execute_sql_tool` for any SQL including `SHOW TABLES`, `DESCRIBE`, `SHOW DATABASES`, etc.

| Tool | Description |
|:---|:---|
| `execute_sql_tool` | Execute any SQL statement (no type restrictions) |
| `batch_execute` | Execute multiple SQL statements sequentially |

### Stateful Connections

Connections are bound to a specific instance. The `conn_id` includes the instance name (e.g. `db1_conn_1`) for easy identification.

| Tool | Description |
|:---|:---|
| `acquire_connection` | Create a persistent connection on a specific instance |
| `release_connection` | Release a specific connection (rolls back uncommitted transactions) |
| `release_all_connections` | Release connections (optionally filtered by instance) |
| `list_connections` | List active connections (optionally filtered by instance) |

### Key Parameters

**create_instance:**

| Parameter | Type | Default | Description |
|:---|:---|:---|:---|
| `name` | string | `""` | Optional name (e.g. "mydb"). Auto-generates `inst_N` if empty |
| `edition` | string | `"standard"` | `"standard"` or `"enterprise"` |
| `ttl_minutes` | int | `720` | Instance TTL in minutes (default 12h) |
| `whitelist` | string | `""` | IP whitelist (e.g. "1.2.3.4,5.6.7.0/24"). Pass "auto" to use the server's public IP |

**execute_sql_tool:**

| Parameter | Type | Default | Description |
|:---|:---|:---|:---|
| `sql` | string | (required) | SQL statement to execute |
| `instance_id` | string | (required) | Target instance |
| `database` | string | `""` | Target database (optional) |
| `conn_id` | string | `""` | Stateful connection ID (optional) |

**batch_execute:**

| Parameter | Type | Default | Description |
|:---|:---|:---|:---|
| `statements` | list[string] | (required) | SQL statements to execute in order |
| `instance_id` | string | (required) | Target instance |
| `database` | string | `""` | Target database (optional) |
| `conn_id` | string | `""` | Stateful connection ID (optional) |

**acquire_connection:**

| Parameter | Type | Default | Description |
|:---|:---|:---|:---|
| `instance_id` | string | (required) | Target instance to connect to |
| `database` | string | `""` | Target database (optional) |
| `idle_timeout_minutes` | int | `30` | Auto-release after idle (minutes) |

## Development

```bash
pip install -e .                     # Install in editable mode
mcp dev polardbx_zero_mcp.py          # Test with MCP Inspector
polardbx-zero-mcp --transport http   # Run HTTP server
```

## License

Apache-2.0

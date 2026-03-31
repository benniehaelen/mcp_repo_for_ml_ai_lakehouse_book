# Databricks Unity Catalog MCP Server

A Model Context Protocol (MCP) server for Databricks Unity Catalog that enables:
- **Metadata exploration** of catalogs, schemas, and tables
- **SQL query execution** on Delta Lake tables
- **Natural language queries** powered by Claude
- **Data visualization** with Plotly charts
- **Multiple client interfaces** (Python, Databricks Notebooks)
- **Progress notifications** for long-running operations via MCP `notifications/progress`

## Features

### MCP Tools
- `list_catalogs` - List all Unity Catalogs
- `list_schemas` - List schemas in a catalog
- `list_tables` - List tables in a schema
- `get_table_info` - Get detailed table metadata including columns, types, and comments
- `execute_sql` - Execute SQL queries on Databricks
- `query_natural_language` - Convert natural language to SQL and execute
- `create_chart` - Generate Plotly charts from query results (bar, line, scatter, pie, histogram, box)

### MCP Resources
- `databricks://catalogs` - List of all catalogs
- `databricks://catalog/{name}` - Schemas in a specific catalog
- `databricks://table/{catalog}/{schema}/{table}` - Detailed table information

### MCP Prompts
- `query-table` - Generate SQL queries for specific tables
- `analyze-data` - Analyze query results and provide insights
- `explore-catalog` - Explore Unity Catalog structure

### Progress Notifications

Long-running tools (`execute_sql`, `query_natural_language`, `create_chart`) emit real-time MCP progress notifications when the client supplies a `progressToken` in the request's `_meta` field. This allows clients to display status updates during multi-step operations.

For example, `query_natural_language` emits five stages:

```
notifications/progress → "Fetching table schema from Unity Catalog..."   (1/5)
notifications/progress → "Generating SQL via Claude..."                  (2/5)
notifications/progress → "Executing generated SQL against warehouse..."  (3/5)
notifications/progress → "Processing query results..."                   (4/5)
notifications/progress → "Natural language query complete"               (5/5)
```

If the client does not request progress (no `progressToken`), notifications are silently skipped.

## Installation

### Using UV (Recommended)

```bash
cd databricks-mcp-server

# Install package and dependencies
uv pip install -e .
```

### Using pip

```bash
pip install -e .
```

Plotly chart export requires `kaleido`:

```bash
pip install kaleido
```

## Configuration

Create a `.env` file in the project root:

```env
# Required: Databricks Configuration
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=your-access-token

# Required for SQL execution
DATABRICKS_WAREHOUSE_ID=your-warehouse-id

# Optional: For natural language queries
ANTHROPIC_API_KEY=your-anthropic-api-key
```

### Getting Databricks Credentials

1. **Host**: Your Databricks workspace URL
2. **Token**: Generate a personal access token from User Settings > Access Tokens
3. **Warehouse ID**: Found in SQL Warehouses > Select warehouse > Connection Details

## Usage

### 1. Running the MCP Server

```bash
# Using the installed entry point
databricks-mcp-server

# Or run directly
python -m databricks_mcp_server.server
```

The server communicates via stdio and implements the MCP protocol.

### 2. Python Client

The client dynamically discovers available tools from the server at connect time. Use `call_tool()` to invoke any tool by name, or use attribute-style access for convenience.

```python
import asyncio
from dotenv import load_dotenv
from databricks_mcp_server.client import DatabricksMCPClient

load_dotenv()

async def main():
    client = DatabricksMCPClient()

    async with client.connect():
        # See what tools the server exposes
        for tool in client.list_tools():
            print(f"- {tool['name']}: {tool['description']}")

        # List catalogs
        catalogs = await client.call_tool("list_catalogs")
        print(catalogs)

        # List schemas in a catalog
        schemas = await client.call_tool("list_schemas", {"catalog": "samples"})
        print(schemas)

        # Execute SQL against the NYC taxi dataset
        result = await client.call_tool("execute_sql", {
            "query": "SELECT pickup_zip, COUNT(*) as trips FROM samples.nyctaxi.trips GROUP BY pickup_zip ORDER BY trips DESC LIMIT 5"
        })
        print(result)

        # Natural language query
        result = await client.call_tool("query_natural_language", {
            "question": "What are the top 5 pickup zip codes by number of trips?",
            "catalog": "samples",
            "schema_name": "nyctaxi",
            "table": "trips",
        })
        print(result)

        # Create a bar chart
        chart = await client.call_tool("create_chart", {
            "query": "SELECT pickup_zip, COUNT(*) as trips FROM samples.nyctaxi.trips WHERE pickup_zip IS NOT NULL GROUP BY pickup_zip ORDER BY trips DESC LIMIT 10",
            "chart_type": "bar",
            "x_column": "pickup_zip",
            "y_column": "trips",
            "title": "Top 10 Pickup Zip Codes",
        })

        # Save chart image
        if 'image_data' in chart:
            import base64
            with open('chart.png', 'wb') as f:
                f.write(base64.b64decode(chart['image_data']))

asyncio.run(main())
```

Tools can also be called via attribute-style access (resolved dynamically from the server):

```python
catalogs = await client.list_catalogs()
schemas = await client.list_schemas(catalog="samples")
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    MCP Server (stdio)                       │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐           │
│  │   Tools    │  │ Resources  │  │  Prompts   │           │
│  └────────────┘  └────────────┘  └────────────┘           │
│                  ┌────────────────────┐                     │
│                  │ ProgressReporter   │                     │
│                  │ (notifications/    │                     │
│                  │  progress)         │                     │
│                  └────────────────────┘                     │
└─────────────────────────────────────────────────────────────┘
                          │
                          └─── Python Client (asyncio, dynamic tool discovery)

                          ↓
┌─────────────────────────────────────────────────────────────┐
│              Databricks Unity Catalog                       │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐           │
│  │  Catalogs  │  │  Schemas   │  │   Tables   │           │
│  └────────────┘  └────────────┘  └────────────┘           │
└─────────────────────────────────────────────────────────────┘
```

## Chart Types

Supported chart types via the `create_chart` tool:
- `bar` - Bar charts for categorical comparisons
- `line` - Line charts for trends over time
- `scatter` - Scatter plots for correlations
- `pie` - Pie charts for proportions
- `histogram` - Distribution analysis
- `box` - Box plots for statistical summaries

## Examples

Working examples are in the `examples/` directory:

- **`basic_usage.py`** - Lists catalogs, schemas, tables, resources, and prompts
- **`natural_language_queries.py`** - NL-to-SQL queries against the NYC taxi dataset
- **`chart_examples.py`** - Generates all 6 chart types plus a multi-chart dashboard
- **`resource_examples.py`** - Demonstrates the MCP resource layer (URI-based catalog browsing)
- **`prompt_examples.py`** - Demonstrates MCP prompt templates (query-table, analyze-data, explore-catalog)
- **`test_databricks.py`** - Direct Databricks SDK connection test

Run any example:

```bash
python examples/basic_usage.py
python examples/chart_examples.py
python examples/chart_examples.py --dashboard
python examples/natural_language_queries.py
python examples/natural_language_queries.py --interactive
python examples/resource_examples.py
python examples/prompt_examples.py
```

## Troubleshooting

### Common Issues

1. **Connection Error**: Verify `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are correct
2. **Warehouse Not Found**: Check `DATABRICKS_WAREHOUSE_ID` is valid and the warehouse is running
3. **Permission Denied**: Ensure your token has access to Unity Catalog and SQL warehouses
4. **NL Queries Failing**: Verify `ANTHROPIC_API_KEY` is set correctly
5. **Charts not rendering**: Install `kaleido` (`pip install kaleido`)

### Debug Mode

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Security Best Practices

1. **Never commit `.env` files** - Use environment variables or secrets management
2. **Use Databricks secrets** - Store credentials in Databricks Secret Scopes
3. **Rotate tokens regularly** - Generate new personal access tokens periodically
4. **Principle of least privilege** - Grant only necessary permissions to service accounts

## License

MIT License - See LICENSE file for details

## Acknowledgments

- Built with [MCP SDK](https://github.com/anthropics/mcp)
- Uses [Databricks SDK for Python](https://github.com/databricks/databricks-sdk-py)
- Powered by [Claude](https://anthropic.com/claude) for natural language queries
- Visualizations by [Plotly](https://plotly.com/)

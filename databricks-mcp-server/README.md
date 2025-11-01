# Databricks Unity Catalog MCP Server

A comprehensive Model Context Protocol (MCP) server for Databricks Unity Catalog that enables:
- **Metadata exploration** of catalogs, schemas, and tables
- **SQL query execution** on Delta Lake tables
- **Natural language queries** powered by Claude
- **Data visualization** with Plotly charts
- **Streaming progress updates** with Server-Sent Events (SSE)
- **Multiple client interfaces** (Python, Databricks Notebooks)

## Features

### 🛠️ MCP Tools
- `list_catalogs` - List all Unity Catalogs
- `list_schemas` - List schemas in a catalog
- `list_tables` - List tables in a schema
- `get_table_info` - Get detailed table metadata including columns, types, and comments
- `execute_sql` - Execute SQL queries on Databricks
- `query_natural_language` - Convert natural language to SQL and execute
- `create_chart` - Generate Plotly charts from query results (bar, line, scatter, pie, histogram, box)

### 📚 MCP Resources
- `databricks://catalogs` - List of all catalogs
- `databricks://catalog/{name}` - Schemas in a specific catalog
- `databricks://table/{catalog}/{schema}/{table}` - Detailed table information

### 💡 MCP Prompts
- `query-table` - Generate SQL queries for specific tables
- `analyze-data` - Analyze query results and provide insights
- `explore-catalog` - Explore Unity Catalog structure

## Installation

### Using UV (Recommended)

```bash
# Clone or create project
cd databricks-mcp-server

# Install dependencies
uv pip install -e .

# Or install directly from requirements
uv pip install mcp databricks-sdk plotly pandas anthropic python-dotenv httpx
```

### Using pip

```bash
pip install -e .
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
2. **Token**: Generate a personal access token from User Settings → Access Tokens
3. **Warehouse ID**: Found in SQL Warehouses → Select warehouse → Connection Details

## Usage

### 1. Running the MCP Server

```bash
# Using the installed script
databricks-mcp-server

# Or run directly
python -m databricks_mcp_server.server
```

The server communicates via stdio and implements the MCP protocol.

### 2. Python Client

```python
import asyncio
from databricks_mcp_server.client import DatabricksMCPClient

async def main():
    client = DatabricksMCPClient()
    
    async with client.connect():
        # List catalogs
        catalogs = await client.list_catalogs()
        print(catalogs)
        
        # List schemas
        schemas = await client.list_schemas("main")
        print(schemas)
        
        # Execute SQL
        result = await client.execute_sql(
            "SELECT * FROM main.default.my_table LIMIT 10"
        )
        print(result)
        
        # Natural language query
        result = await client.query_natural_language(
            question="What are the top 5 records by revenue?",
            catalog="main",
            schema="default",
            table="sales"
        )
        print(result['response'])
        
        # Create a chart
        chart = await client.create_chart(
            query="SELECT category, SUM(revenue) as total FROM main.default.sales GROUP BY category",
            chart_type="bar",
            x_column="category",
            y_column="total",
            title="Revenue by Category"
        )
        
        # Save chart image
        if 'image_data' in chart:
            import base64
            with open('chart.png', 'wb') as f:
                f.write(base64.b64decode(chart['image_data']))

asyncio.run(main())
```

### 3. Databricks Notebook Client

The notebook client can be used directly within Databricks notebooks without running a separate server:

```python
# Install in notebook
%pip install mcp anthropic plotly pandas databricks-sdk

# Import and initialize
from databricks_mcp_notebook_client import NotebookMCPClient

client = NotebookMCPClient(
    warehouse_id="your-warehouse-id",
    anthropic_api_key="your-api-key"  # Optional, for NL queries
)

# List catalogs
catalogs_df = client.list_catalogs()
display(catalogs_df)

# Execute SQL
df = client.execute_sql("SELECT * FROM main.default.table LIMIT 10")
display(df)

# Natural language query
result = client.query_natural_language(
    question="Show me top customers by order count",
    catalog="main",
    schema="default",
    table="orders"
)
print(f"Generated SQL: {result['sql']}")
display(result['data'])

# Create chart
fig = client.create_chart(
    query="SELECT date, revenue FROM main.default.sales",
    chart_type="line",
    x="date",
    y="revenue",
    title="Daily Revenue"
)
fig.show()
```

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    MCP Server (stdio)                        │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│  │   Tools    │  │ Resources  │  │  Prompts   │            │
│  └────────────┘  └────────────┘  └────────────┘            │
└─────────────────────────────────────────────────────────────┘
                          │
                          ├─── Python Client (asyncio)
                          │
                          └─── Notebook Client (direct SDK)
                          
                          ↓
┌─────────────────────────────────────────────────────────────┐
│              Databricks Unity Catalog                        │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐            │
│  │  Catalogs  │  │  Schemas   │  │   Tables   │            │
│  └────────────┘  └────────────┘  └────────────┘            │
└─────────────────────────────────────────────────────────────┘
```

## Advanced Features

### Streaming Progress (Server-Sent Events)

The MCP server supports streaming progress updates for long-running operations:

```python
# Progress updates are automatically streamed for:
# - Large query executions
# - Natural language query processing
# - Chart generation with large datasets
```

### Custom Chart Types

Supported chart types:
- `bar` - Bar charts for categorical comparisons
- `line` - Line charts for trends over time
- `scatter` - Scatter plots for correlations
- `pie` - Pie charts for proportions
- `histogram` - Distribution analysis
- `box` - Box plots for statistical summaries

### Batch Operations

Process multiple tables efficiently:

```python
# Using the notebook client
tables = [
    ("main", "default", "table1"),
    ("main", "default", "table2"),
    ("main", "sales", "orders")
]

for catalog, schema, table in tables:
    df = client.execute_sql(f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 100")
    print(f"Processed {catalog}.{schema}.{table}: {len(df)} rows")
```

## Examples

### Example 1: Explore Catalog Structure

```python
async def explore():
    async with client.connect():
        # List all resources
        resources = await client.list_resources()
        for r in resources:
            print(f"{r['name']}: {r['uri']}")
        
        # Read catalog resource
        catalogs = await client.read_resource("databricks://catalogs")
        print(catalogs)
```

### Example 2: Query with Natural Language

```python
async def nl_query():
    async with client.connect():
        result = await client.query_natural_language(
            question="What were our top 10 products by revenue last month?",
            catalog="main",
            schema="sales",
            table="product_sales"
        )
        print(result['response'])
```

### Example 3: Create Dashboard

```python
async def create_dashboard():
    async with client.connect():
        # Revenue by category
        chart1 = await client.create_chart(
            query="SELECT category, SUM(revenue) as total FROM main.sales.orders GROUP BY category",
            chart_type="pie",
            x_column="category",
            y_column="total",
            title="Revenue by Category"
        )
        
        # Daily trends
        chart2 = await client.create_chart(
            query="SELECT date, COUNT(*) as orders FROM main.sales.orders GROUP BY date ORDER BY date",
            chart_type="line",
            x_column="date",
            y_column="orders",
            title="Daily Orders"
        )
```

## Troubleshooting

### Common Issues

1. **Connection Error**: Verify `DATABRICKS_HOST` and `DATABRICKS_TOKEN` are correct
2. **Warehouse Not Found**: Check `DATABRICKS_WAREHOUSE_ID` is valid and warehouse is running
3. **Permission Denied**: Ensure your token has access to Unity Catalog and SQL warehouses
4. **NL Queries Failing**: Verify `ANTHROPIC_API_KEY` is set correctly

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

# Quick Start Guide

Get up and running with the Databricks MCP Server in 5 minutes!

## Prerequisites

- Python 3.11 or higher
- Access to a Databricks workspace
- A SQL warehouse (for query execution)
- UV package manager (recommended) or pip

## Step 1: Installation

### Option A: Using UV (Recommended)

```bash
# Navigate to project directory
cd databricks-mcp-server

# Install with UV
uv pip install -e .
```

### Option B: Using pip

```bash
pip install -e .
```

## Step 2: Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` with your credentials:
```env
DATABRICKS_HOST=https://your-workspace.cloud.databricks.com
DATABRICKS_TOKEN=dapi1234567890abcdef
DATABRICKS_WAREHOUSE_ID=abcdef123456
ANTHROPIC_API_KEY=sk-ant-api03-xxxxx  # Optional, for NL queries
```

### Getting Your Credentials

**Databricks Host:**
- Your workspace URL (e.g., `https://dbc-12345678-abcd.cloud.databricks.com`)

**Databricks Token:**
1. Go to User Settings in your Databricks workspace
2. Click "Developer" → "Access tokens"
3. Click "Generate new token"
4. Copy the generated token

**Warehouse ID:**
1. Go to "SQL Warehouses" in your workspace
2. Select your warehouse
3. Go to "Connection details"
4. Copy the "Server hostname" (the part after `/sql/1.0/warehouses/`)

**Anthropic API Key (Optional):**
- Required only for natural language queries
- Get it from https://console.anthropic.com/

## Step 3: Test Connection

Run the basic usage example:

```bash
python examples/basic_usage.py
```

Expected output:
```
================================================================================
Databricks MCP Client - Basic Usage Examples
================================================================================

1. Listing all Unity Catalogs...
--------------------------------------------------------------------------------
{
  "catalogs": [
    {
      "name": "main",
      "comment": "Main catalog",
      "owner": "admin"
    }
  ]
}
...
```

## Step 4: Try Different Clients

### A. Python Async Client

```python
import asyncio
from databricks_mcp_server.client import DatabricksMCPClient

async def main():
    client = DatabricksMCPClient()
    async with client.connect():
        # List catalogs
        catalogs = await client.list_catalogs()
        print(catalogs)
        
        # Execute SQL
        result = await client.execute_sql(
            "SELECT * FROM main.default.my_table LIMIT 10"
        )
        print(result)

asyncio.run(main())
```

### B. Databricks Notebook Client

1. Import the notebook file `databricks_mcp_notebook_client.py` into your Databricks workspace
2. Run the cells to:
   - Install dependencies
   - Initialize the client
   - Execute queries and create visualizations

## Step 5: Run Examples

### Basic Operations
```bash
python examples/basic_usage.py
```

### Natural Language Queries
```bash
# Make sure ANTHROPIC_API_KEY is set
python examples/natural_language_queries.py

# Interactive mode
python examples/natural_language_queries.py --interactive
```

### Create Charts
```bash
python examples/chart_examples.py

# Create a dashboard
python examples/chart_examples.py --dashboard
```

## Common Use Cases

### 1. Explore Your Data
```python
async with client.connect():
    # List all catalogs
    catalogs = await client.list_catalogs()
    
    # List schemas in a catalog
    schemas = await client.list_schemas("main")
    
    # List tables in a schema
    tables = await client.list_tables("main", "default")
    
    # Get detailed table info
    info = await client.get_table_info("main", "default", "my_table")
```

### 2. Query with SQL
```python
async with client.connect():
    result = await client.execute_sql("""
        SELECT category, COUNT(*) as count
        FROM main.default.products
        GROUP BY category
        ORDER BY count DESC
        LIMIT 10
    """)
    print(result)
```

### 3. Query with Natural Language
```python
async with client.connect():
    result = await client.query_natural_language(
        question="What are the top 5 products by revenue?",
        catalog="main",
        schema="default",
        table="sales"
    )
    print(result['response'])
```

### 4. Create Visualizations
```python
async with client.connect():
    chart = await client.create_chart(
        query="SELECT date, SUM(revenue) as total FROM main.default.sales GROUP BY date",
        chart_type="line",
        x_column="date",
        y_column="total",
        title="Daily Revenue Trend"
    )
    
    # Save the chart
    if 'image_data' in chart:
        import base64
        with open('chart.png', 'wb') as f:
            f.write(base64.b64decode(chart['image_data']))
```

## Troubleshooting

### Connection Issues

**Problem:** `Error: Databricks client not initialized`
- **Solution:** Check your `.env` file has correct `DATABRICKS_HOST` and `DATABRICKS_TOKEN`

**Problem:** `Warehouse not found`
- **Solution:** Verify `DATABRICKS_WAREHOUSE_ID` is correct and the warehouse is running

### Query Issues

**Problem:** `Table not found`
- **Solution:** Verify the table exists and you have access permissions

**Problem:** `Permission denied`
- **Solution:** Ensure your Databricks token has appropriate permissions for Unity Catalog

### Natural Language Query Issues

**Problem:** `Anthropic client not initialized`
- **Solution:** Set `ANTHROPIC_API_KEY` in your `.env` file

**Problem:** Generated SQL is incorrect
- **Solution:** The table schema is automatically provided to Claude. Ensure your table has descriptive column names and comments.

## Next Steps

1. **Explore the Examples**: Check out all examples in the `examples/` directory
2. **Read the Full Documentation**: See `README.md` for comprehensive documentation
3. **Customize for Your Use Case**: Modify the examples or create your own scripts
4. **Integrate with Your Tools**: Use the MCP server with Claude Desktop or other MCP-compatible tools

## Support

- **GitHub Issues**: Report bugs or request features
- **Databricks Community**: Ask questions about Databricks integration
- **MCP Documentation**: Learn more about the Model Context Protocol

## What's Next?

Now that you're set up, you can:
- ✅ Query your Unity Catalog metadata
- ✅ Execute SQL queries on Delta Lake tables
- ✅ Use natural language to generate queries
- ✅ Create beautiful visualizations
- ✅ Build data pipelines and workflows

Happy querying! 🚀

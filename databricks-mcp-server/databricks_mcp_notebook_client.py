# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks MCP Client for Notebooks
# MAGIC 
# MAGIC This notebook provides a client interface to interact with the Databricks MCP Server
# MAGIC from within Databricks notebooks. It allows you to:
# MAGIC - Query Unity Catalog metadata
# MAGIC - Execute SQL queries
# MAGIC - Use natural language to generate and execute queries
# MAGIC - Create visualizations with Plotly
# MAGIC
# MAGIC ## Setup
# MAGIC 1. Install required packages
# MAGIC 2. Configure environment variables
# MAGIC 3. Use the NotebookMCPClient class

# COMMAND ----------

# MAGIC %pip install mcp anthropic plotly pandas kaleido --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
from typing import Dict, Any, Optional, List
import asyncio
from dataclasses import dataclass
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from anthropic import Anthropic

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC 
# MAGIC Set up your credentials as Databricks secrets or environment variables:

# COMMAND ----------

# Configuration - Update these or use Databricks secrets
DATABRICKS_HOST = dbutils.secrets.get(scope="databricks-mcp", key="host") if dbutils.secrets.listScopes() else os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = dbutils.secrets.get(scope="databricks-mcp", key="token") if dbutils.secrets.listScopes() else os.getenv("DATABRICKS_TOKEN")
DATABRICKS_WAREHOUSE_ID = dbutils.secrets.get(scope="databricks-mcp", key="warehouse-id") if dbutils.secrets.listScopes() else os.getenv("DATABRICKS_WAREHOUSE_ID")
ANTHROPIC_API_KEY = dbutils.secrets.get(scope="databricks-mcp", key="anthropic-api-key") if dbutils.secrets.listScopes() else os.getenv("ANTHROPIC_API_KEY")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notebook MCP Client
# MAGIC 
# MAGIC This is a simplified client that works directly in Databricks notebooks without needing
# MAGIC to run a separate MCP server process.

# COMMAND ----------

class NotebookMCPClient:
    """
    Simplified MCP client for Databricks notebooks.
    Uses the Databricks SDK directly within the notebook environment.
    """
    
    def __init__(self, warehouse_id: Optional[str] = None, anthropic_api_key: Optional[str] = None):
        """
        Initialize the notebook client
        
        Args:
            warehouse_id: SQL warehouse ID for query execution
            anthropic_api_key: Anthropic API key for NL queries
        """
        self.warehouse_id = warehouse_id or DATABRICKS_WAREHOUSE_ID
        self.anthropic_client = Anthropic(api_key=anthropic_api_key) if anthropic_api_key else None
        
        # Import Databricks SDK
        from databricks.sdk import WorkspaceClient
        from databricks.sdk.core import Config
        
        # Initialize workspace client
        config = Config(
            host=DATABRICKS_HOST or dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().get(),
            token=DATABRICKS_TOKEN or dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        )
        self.workspace_client = WorkspaceClient(config=config)
    
    def list_catalogs(self) -> pd.DataFrame:
        """List all Unity Catalogs"""
        catalogs = list(self.workspace_client.catalogs.list())
        return pd.DataFrame([
            {
                "name": c.name,
                "comment": c.comment,
                "owner": c.owner,
                "created_at": str(c.created_at) if c.created_at else None
            }
            for c in catalogs
        ])
    
    def list_schemas(self, catalog: str) -> pd.DataFrame:
        """List all schemas in a catalog"""
        schemas = list(self.workspace_client.schemas.list(catalog_name=catalog))
        return pd.DataFrame([
            {
                "catalog": catalog,
                "name": s.name,
                "comment": s.comment,
                "owner": s.owner,
                "created_at": str(s.created_at) if s.created_at else None
            }
            for s in schemas
        ])
    
    def list_tables(self, catalog: str, schema: str) -> pd.DataFrame:
        """List all tables in a schema"""
        tables = list(self.workspace_client.tables.list(
            catalog_name=catalog,
            schema_name=schema
        ))
        return pd.DataFrame([
            {
                "catalog": catalog,
                "schema": schema,
                "name": t.name,
                "table_type": t.table_type.value if t.table_type else None,
                "comment": t.comment,
                "owner": t.owner
            }
            for t in tables
        ])
    
    def get_table_info(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Get detailed information about a table"""
        table_info = self.workspace_client.tables.get(
            full_name=f"{catalog}.{schema}.{table}"
        )
        
        return {
            "name": table_info.name,
            "catalog_name": table_info.catalog_name,
            "schema_name": table_info.schema_name,
            "table_type": table_info.table_type.value if table_info.table_type else None,
            "data_source_format": table_info.data_source_format.value if table_info.data_source_format else None,
            "columns": [
                {
                    "name": col.name,
                    "type_name": col.type_name.value if col.type_name else None,
                    "type_text": col.type_text,
                    "comment": col.comment,
                    "nullable": col.nullable,
                    "position": col.position
                }
                for col in (table_info.columns or [])
            ],
            "owner": table_info.owner,
            "comment": table_info.comment
        }
    
    def execute_sql(self, query: str, warehouse_id: Optional[str] = None) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame
        
        Args:
            query: SQL query to execute
            warehouse_id: Optional warehouse ID (uses default if not provided)
        
        Returns:
            DataFrame with query results
        """
        wh_id = warehouse_id or self.warehouse_id
        
        if not wh_id:
            raise ValueError("No warehouse_id provided and DATABRICKS_WAREHOUSE_ID not set")
        
        # Execute query
        response = self.workspace_client.statement_execution.execute_statement(
            warehouse_id=wh_id,
            statement=query,
            wait_timeout="30s"
        )
        
        # Convert to DataFrame
        if response.result and response.result.data_array:
            columns = [col.name for col in (response.manifest.schema.columns or [])]
            df = pd.DataFrame(response.result.data_array, columns=columns)
            return df
        else:
            return pd.DataFrame()
    
    def query_natural_language(
        self,
        question: str,
        catalog: str,
        schema: str,
        table: str,
        warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert natural language question to SQL and execute
        
        Args:
            question: Natural language question
            catalog: Catalog name
            schema: Schema name
            table: Table name
            warehouse_id: Optional warehouse ID
        
        Returns:
            Dictionary with 'sql', 'data' (DataFrame), and 'explanation'
        """
        if not self.anthropic_client:
            raise ValueError("Anthropic client not initialized. Provide anthropic_api_key.")
        
        # Get table schema
        table_info = self.get_table_info(catalog, schema, table)
        
        schema_text = "\n".join([
            f"- {col['name']} ({col['type_text'] or col['type_name']}): {col['comment'] or 'No description'}"
            for col in table_info['columns']
        ])
        
        # Generate SQL using Claude
        prompt = f"""Convert this natural language question to a SQL query for Databricks Delta Lake.

Table: {catalog}.{schema}.{table}
Description: {table_info['comment'] or 'No description'}

Schema:
{schema_text}

Question: {question}

Provide only the SQL query without any explanation or markdown formatting."""
        
        message = self.anthropic_client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}]
        )
        
        sql_query = message.content[0].text.strip()
        
        # Remove markdown code blocks if present
        if sql_query.startswith("```"):
            lines = sql_query.split("\n")
            sql_query = "\n".join(lines[1:-1]) if len(lines) > 2 else sql_query
        
        # Execute query
        df = self.execute_sql(sql_query, warehouse_id)
        
        return {
            "sql": sql_query,
            "data": df,
            "question": question
        }
    
    def create_chart(
        self,
        query: str = None,
        data: pd.DataFrame = None,
        chart_type: str = "bar",
        x: Optional[str] = None,
        y: Optional[str] = None,
        title: str = "Chart",
        **kwargs
    ) -> go.Figure:
        """
        Create a Plotly chart from query results or DataFrame
        
        Args:
            query: SQL query (if data not provided)
            data: DataFrame (if query not provided)
            chart_type: Type of chart (bar, line, scatter, pie, histogram, box)
            x: X-axis column name
            y: Y-axis column name
            title: Chart title
            **kwargs: Additional arguments passed to Plotly
        
        Returns:
            Plotly Figure object
        """
        # Get data
        if data is None and query:
            data = self.execute_sql(query)
        elif data is None:
            raise ValueError("Must provide either 'query' or 'data'")
        
        if data.empty:
            raise ValueError("No data to chart")
        
        # Auto-detect columns if not specified
        if not x and len(data.columns) > 0:
            x = data.columns[0]
        if not y and len(data.columns) > 1:
            y = data.columns[1]
        
        # Create chart
        if chart_type == "bar":
            fig = px.bar(data, x=x, y=y, title=title, **kwargs)
        elif chart_type == "line":
            fig = px.line(data, x=x, y=y, title=title, **kwargs)
        elif chart_type == "scatter":
            fig = px.scatter(data, x=x, y=y, title=title, **kwargs)
        elif chart_type == "pie":
            fig = px.pie(data, names=x, values=y, title=title, **kwargs)
        elif chart_type == "histogram":
            fig = px.histogram(data, x=x, title=title, **kwargs)
        elif chart_type == "box":
            fig = px.box(data, y=y, title=title, **kwargs)
        else:
            raise ValueError(f"Unsupported chart type: {chart_type}")
        
        return fig
    
    def explore_catalog(self, catalog: str = None) -> Dict[str, Any]:
        """
        Get a complete overview of a catalog or all catalogs
        
        Args:
            catalog: Specific catalog to explore (None for all)
        
        Returns:
            Dictionary with catalog structure
        """
        if catalog:
            schemas = self.list_schemas(catalog)
            structure = {
                "catalog": catalog,
                "schemas": {}
            }
            
            for _, schema_row in schemas.iterrows():
                schema_name = schema_row['name']
                tables = self.list_tables(catalog, schema_name)
                structure["schemas"][schema_name] = {
                    "comment": schema_row['comment'],
                    "tables": tables['name'].tolist()
                }
            
            return structure
        else:
            catalogs = self.list_catalogs()
            return {
                "catalogs": [
                    {
                        "name": row['name'],
                        "comment": row['comment'],
                        "owner": row['owner']
                    }
                    for _, row in catalogs.iterrows()
                ]
            }

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage Examples

# COMMAND ----------

# Initialize the client
client = NotebookMCPClient(
    warehouse_id=DATABRICKS_WAREHOUSE_ID,
    anthropic_api_key=ANTHROPIC_API_KEY
)

# COMMAND ----------

# Example 1: List all catalogs
print("=== Unity Catalogs ===")
catalogs_df = client.list_catalogs()
display(catalogs_df)

# COMMAND ----------

# Example 2: List schemas in a catalog
# Replace 'main' with your catalog name
catalog_name = "main"
print(f"=== Schemas in {catalog_name} ===")
schemas_df = client.list_schemas(catalog_name)
display(schemas_df)

# COMMAND ----------

# Example 3: List tables in a schema
# Replace with your catalog and schema names
catalog_name = "main"
schema_name = "default"
print(f"=== Tables in {catalog_name}.{schema_name} ===")
tables_df = client.list_tables(catalog_name, schema_name)
display(tables_df)

# COMMAND ----------

# Example 4: Get detailed table information
# Replace with your table details
catalog_name = "main"
schema_name = "default"
table_name = "your_table"

try:
    table_info = client.get_table_info(catalog_name, schema_name, table_name)
    print(f"=== Table Info: {catalog_name}.{schema_name}.{table_name} ===")
    print(json.dumps(table_info, indent=2))
except Exception as e:
    print(f"Table not found or error: {e}")

# COMMAND ----------

# Example 5: Execute SQL query
query = """
SELECT * 
FROM main.default.your_table 
LIMIT 10
"""

try:
    result_df = client.execute_sql(query)
    print(f"Query returned {len(result_df)} rows")
    display(result_df)
except Exception as e:
    print(f"Error executing query: {e}")

# COMMAND ----------

# Example 6: Natural language query
# Requires ANTHROPIC_API_KEY to be set
try:
    result = client.query_natural_language(
        question="What are the top 5 records by value?",
        catalog="main",
        schema="default",
        table="your_table"
    )
    
    print("=== Generated SQL ===")
    print(result['sql'])
    print("\n=== Results ===")
    display(result['data'])
except Exception as e:
    print(f"Error with NL query: {e}")

# COMMAND ----------

# Example 7: Create a chart
query = """
SELECT category, COUNT(*) as count
FROM main.default.your_table
GROUP BY category
ORDER BY count DESC
LIMIT 10
"""

try:
    fig = client.create_chart(
        query=query,
        chart_type="bar",
        x="category",
        y="count",
        title="Top 10 Categories",
        color="category"
    )
    fig.show()
except Exception as e:
    print(f"Error creating chart: {e}")

# COMMAND ----------

# Example 8: Create chart from DataFrame
try:
    df = client.execute_sql("SELECT * FROM main.default.your_table LIMIT 100")
    
    fig = client.create_chart(
        data=df,
        chart_type="scatter",
        x="column1",
        y="column2",
        title="Scatter Plot Example"
    )
    fig.show()
except Exception as e:
    print(f"Error: {e}")

# COMMAND ----------

# Example 9: Explore entire catalog structure
catalog_to_explore = "main"

try:
    structure = client.explore_catalog(catalog_to_explore)
    print(f"=== Catalog Structure: {catalog_to_explore} ===")
    print(json.dumps(structure, indent=2))
except Exception as e:
    print(f"Error exploring catalog: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Usage: Interactive Querying

# COMMAND ----------

def interactive_query(client: NotebookMCPClient, catalog: str, schema: str, table: str):
    """
    Interactive helper function for querying tables with natural language
    """
    
    # Get table info
    table_info = client.get_table_info(catalog, schema, table)
    
    print(f"📊 Table: {catalog}.{schema}.{table}")
    print(f"📝 Description: {table_info.get('comment', 'No description')}")
    print(f"\n📋 Columns ({len(table_info['columns'])}):")
    
    for col in table_info['columns']:
        print(f"  • {col['name']} ({col['type_text'] or col['type_name']})")
        if col['comment']:
            print(f"    └─ {col['comment']}")
    
    print("\n" + "="*80)
    print("💡 You can now ask questions about this table using natural language!")
    print("="*80)

# Usage
# interactive_query(client, "main", "default", "your_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Utility Functions

# COMMAND ----------

def save_chart_as_image(fig: go.Figure, filename: str = "chart.png"):
    """Save a Plotly figure as an image file"""
    fig.write_image(filename)
    print(f"Chart saved as {filename}")

def export_to_csv(df: pd.DataFrame, filename: str = "export.csv"):
    """Export DataFrame to CSV"""
    df.to_csv(filename, index=False)
    print(f"Data exported to {filename}")

def quick_summary(df: pd.DataFrame):
    """Display quick summary statistics of a DataFrame"""
    print("=== DataFrame Summary ===")
    print(f"Rows: {len(df)}")
    print(f"Columns: {len(df.columns)}")
    print(f"\nColumn Types:")
    print(df.dtypes)
    print(f"\nSummary Statistics:")
    display(df.describe())
    print(f"\nFirst 5 rows:")
    display(df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Batch Operations

# COMMAND ----------

def batch_query_tables(client: NotebookMCPClient, table_list: List[tuple]) -> Dict[str, pd.DataFrame]:
    """
    Execute the same query pattern on multiple tables
    
    Args:
        client: NotebookMCPClient instance
        table_list: List of (catalog, schema, table) tuples
    
    Returns:
        Dictionary mapping table names to DataFrames
    """
    results = {}
    
    for catalog, schema, table in table_list:
        full_name = f"{catalog}.{schema}.{table}"
        try:
            query = f"SELECT * FROM {full_name} LIMIT 100"
            df = client.execute_sql(query)
            results[full_name] = df
            print(f"✓ {full_name}: {len(df)} rows")
        except Exception as e:
            print(f"✗ {full_name}: {e}")
    
    return results

# Example usage:
# tables = [
#     ("main", "default", "table1"),
#     ("main", "default", "table2"),
#     ("main", "default", "table3")
# ]
# results = batch_query_tables(client, tables)

# COMMAND ----------

# MAGIC %md
# MAGIC ## End of Notebook
# MAGIC 
# MAGIC You now have a complete toolkit for:
# MAGIC - Exploring Unity Catalog
# MAGIC - Executing SQL and natural language queries  
# MAGIC - Creating visualizations
# MAGIC - Analyzing data
# MAGIC 
# MAGIC Feel free to customize and extend these examples for your specific use cases!

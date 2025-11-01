"""
Databricks Unity Catalog MCP Server
Provides tools, resources, and prompts for querying Databricks Unity Catalog
"""
import asyncio
import json
import logging
import os
from typing import Any, Optional, Sequence
from contextlib import asynccontextmanager

from anthropic import Anthropic
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    Prompt,
    PromptMessage,
    GetPromptResult,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabricksMCPServer:
    """MCP Server for Databricks Unity Catalog operations"""
    
    def __init__(self):
        self.app = Server("databricks-mcp-server")
        self.workspace_client: Optional[WorkspaceClient] = None
        self.anthropic_client: Optional[Anthropic] = None
        self._setup_handlers()
        
    def _setup_handlers(self):
        """Setup all MCP protocol handlers"""
        
        @self.app.list_resources()
        async def list_resources() -> list[Resource]:
            """List available Unity Catalog resources"""
            resources = []
            
            if not self.workspace_client:
                return resources
            
            try:
                # List catalogs
                resources.append(Resource(
                    uri="databricks://catalogs",
                    name="Unity Catalog - Catalogs",
                    mimeType="application/json",
                    description="List of all catalogs in Unity Catalog"
                ))
                
                # Add catalog/schema resources
                catalogs = list(self.workspace_client.catalogs.list())
                for catalog in catalogs:
                    catalog_name = catalog.name
                    resources.append(Resource(
                        uri=f"databricks://catalog/{catalog_name}",
                        name=f"Catalog: {catalog_name}",
                        mimeType="application/json",
                        description=f"Schemas in catalog {catalog_name}"
                    ))
                    
            except Exception as e:
                logger.error(f"Error listing resources: {e}")
            
            return resources
        
        @self.app.read_resource()
        async def read_resource(uri: str) -> str:
            """Read a Unity Catalog resource"""
            if not self.workspace_client:
                return json.dumps({"error": "Databricks client not initialized"})
            
            try:
                if uri == "databricks://catalogs":
                    catalogs = list(self.workspace_client.catalogs.list())
                    return json.dumps({
                        "catalogs": [
                            {
                                "name": c.name,
                                "comment": c.comment,
                                "created_at": str(c.created_at) if c.created_at else None,
                                "owner": c.owner
                            }
                            for c in catalogs
                        ]
                    }, indent=2)
                
                elif uri.startswith("databricks://catalog/"):
                    catalog_name = uri.replace("databricks://catalog/", "")
                    schemas = list(self.workspace_client.schemas.list(catalog_name=catalog_name))
                    return json.dumps({
                        "catalog": catalog_name,
                        "schemas": [
                            {
                                "name": s.name,
                                "comment": s.comment,
                                "created_at": str(s.created_at) if s.created_at else None,
                                "owner": s.owner
                            }
                            for s in schemas
                        ]
                    }, indent=2)
                
                elif uri.startswith("databricks://table/"):
                    parts = uri.replace("databricks://table/", "").split("/")
                    if len(parts) == 3:
                        catalog, schema, table = parts
                        table_info = self.workspace_client.tables.get(
                            full_name=f"{catalog}.{schema}.{table}"
                        )
                        return json.dumps({
                            "name": table_info.name,
                            "catalog_name": table_info.catalog_name,
                            "schema_name": table_info.schema_name,
                            "table_type": table_info.table_type.value if table_info.table_type else None,
                            "columns": [
                                {
                                    "name": col.name,
                                    "type_name": col.type_name.value if col.type_name else None,
                                    "comment": col.comment
                                }
                                for col in (table_info.columns or [])
                            ],
                            "owner": table_info.owner,
                            "comment": table_info.comment
                        }, indent=2)
                
                return json.dumps({"error": "Invalid resource URI"})
                
            except Exception as e:
                logger.error(f"Error reading resource {uri}: {e}")
                return json.dumps({"error": str(e)})
        
        @self.app.list_prompts()
        async def list_prompts() -> list[Prompt]:
            """List available prompt templates"""
            return [
                Prompt(
                    name="query-table",
                    description="Generate a SQL query for a Unity Catalog table",
                    arguments=[
                        {"name": "catalog", "description": "Catalog name", "required": True},
                        {"name": "schema", "description": "Schema name", "required": True},
                        {"name": "table", "description": "Table name", "required": True},
                        {"name": "question", "description": "Natural language question", "required": True}
                    ]
                ),
                Prompt(
                    name="analyze-data",
                    description="Analyze data from a query result",
                    arguments=[
                        {"name": "data_description", "description": "Description of the data", "required": True}
                    ]
                ),
                Prompt(
                    name="explore-catalog",
                    description="Explore Unity Catalog structure",
                    arguments=[
                        {"name": "catalog", "description": "Catalog name to explore", "required": False}
                    ]
                )
            ]
        
        @self.app.get_prompt()
        async def get_prompt(name: str, arguments: dict[str, str] | None) -> GetPromptResult:
            """Get a specific prompt template"""
            args = arguments or {}
            
            if name == "query-table":
                catalog = args.get("catalog", "")
                schema = args.get("schema", "")
                table = args.get("table", "")
                question = args.get("question", "")
                
                return GetPromptResult(
                    description=f"Generate SQL query for {catalog}.{schema}.{table}",
                    messages=[
                        PromptMessage(
                            role="user",
                            content=TextContent(
                                type="text",
                                text=f"""Generate a SQL query to answer the following question about the table {catalog}.{schema}.{table}:

Question: {question}

Please provide a valid SQL query that can be executed on Databricks Delta Lake."""
                            )
                        )
                    ]
                )
            
            elif name == "analyze-data":
                data_desc = args.get("data_description", "")
                return GetPromptResult(
                    description="Analyze data from query results",
                    messages=[
                        PromptMessage(
                            role="user",
                            content=TextContent(
                                type="text",
                                text=f"""Analyze the following data and provide insights:

{data_desc}

Please provide:
1. Key findings
2. Notable patterns or trends
3. Recommendations for further analysis"""
                            )
                        )
                    ]
                )
            
            elif name == "explore-catalog":
                catalog = args.get("catalog", "")
                prompt_text = f"Explore the Unity Catalog structure"
                if catalog:
                    prompt_text += f" for catalog: {catalog}"
                
                return GetPromptResult(
                    description="Explore Unity Catalog",
                    messages=[
                        PromptMessage(
                            role="user",
                            content=TextContent(
                                type="text",
                                text=prompt_text
                            )
                        )
                    ]
                )
            
            raise ValueError(f"Unknown prompt: {name}")
        
        @self.app.list_tools()
        async def list_tools() -> list[Tool]:
            """List available tools"""
            return [
                Tool(
                    name="list_catalogs",
                    description="List all catalogs in Unity Catalog",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                    }
                ),
                Tool(
                    name="list_schemas",
                    description="List all schemas in a catalog",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "catalog": {
                                "type": "string",
                                "description": "Catalog name"
                            }
                        },
                        "required": ["catalog"]
                    }
                ),
                Tool(
                    name="list_tables",
                    description="List all tables in a schema",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "catalog": {
                                "type": "string",
                                "description": "Catalog name"
                            },
                            "schema": {
                                "type": "string",
                                "description": "Schema name"
                            }
                        },
                        "required": ["catalog", "schema"]
                    }
                ),
                Tool(
                    name="get_table_info",
                    description="Get detailed information about a table",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "catalog": {"type": "string", "description": "Catalog name"},
                            "schema": {"type": "string", "description": "Schema name"},
                            "table": {"type": "string", "description": "Table name"}
                        },
                        "required": ["catalog", "schema", "table"]
                    }
                ),
                Tool(
                    name="execute_sql",
                    description="Execute a SQL query on Databricks",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to execute"
                            },
                            "warehouse_id": {
                                "type": "string",
                                "description": "SQL warehouse ID (optional, uses default if not provided)"
                            }
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="query_natural_language",
                    description="Convert natural language to SQL and execute",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "question": {
                                "type": "string",
                                "description": "Natural language question"
                            },
                            "catalog": {"type": "string", "description": "Catalog name"},
                            "schema": {"type": "string", "description": "Schema name"},
                            "table": {"type": "string", "description": "Table name"},
                            "warehouse_id": {
                                "type": "string",
                                "description": "SQL warehouse ID (optional)"
                            }
                        },
                        "required": ["question", "catalog", "schema", "table"]
                    }
                ),
                Tool(
                    name="create_chart",
                    description="Create a Plotly chart from query results",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to get data for chart"
                            },
                            "chart_type": {
                                "type": "string",
                                "enum": ["bar", "line", "scatter", "pie", "histogram", "box"],
                                "description": "Type of chart to create"
                            },
                            "x_column": {
                                "type": "string",
                                "description": "Column name for X axis"
                            },
                            "y_column": {
                                "type": "string",
                                "description": "Column name for Y axis"
                            },
                            "title": {
                                "type": "string",
                                "description": "Chart title"
                            },
                            "warehouse_id": {
                                "type": "string",
                                "description": "SQL warehouse ID (optional)"
                            }
                        },
                        "required": ["query", "chart_type"]
                    }
                )
            ]
        
        @self.app.call_tool()
        async def call_tool(name: str, arguments: Any) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
            """Execute a tool"""
            
            try:
                if name == "list_catalogs":
                    return await self._list_catalogs()
                
                elif name == "list_schemas":
                    return await self._list_schemas(arguments["catalog"])
                
                elif name == "list_tables":
                    return await self._list_tables(arguments["catalog"], arguments["schema"])
                
                elif name == "get_table_info":
                    return await self._get_table_info(
                        arguments["catalog"],
                        arguments["schema"],
                        arguments["table"]
                    )
                
                elif name == "execute_sql":
                    return await self._execute_sql(
                        arguments["query"],
                        arguments.get("warehouse_id")
                    )
                
                elif name == "query_natural_language":
                    return await self._query_natural_language(
                        arguments["question"],
                        arguments["catalog"],
                        arguments["schema"],
                        arguments["table"],
                        arguments.get("warehouse_id")
                    )
                
                elif name == "create_chart":
                    return await self._create_chart(
                        arguments["query"],
                        arguments["chart_type"],
                        arguments.get("x_column"),
                        arguments.get("y_column"),
                        arguments.get("title", "Chart"),
                        arguments.get("warehouse_id")
                    )
                
                else:
                    return [TextContent(type="text", text=f"Unknown tool: {name}")]
                    
            except Exception as e:
                logger.error(f"Error executing tool {name}: {e}", exc_info=True)
                return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    async def _list_catalogs(self) -> Sequence[TextContent]:
        """List all catalogs"""
        if not self.workspace_client:
            return [TextContent(type="text", text="Error: Databricks client not initialized")]
        
        catalogs = list(self.workspace_client.catalogs.list())
        result = {
            "catalogs": [
                {
                    "name": c.name,
                    "comment": c.comment,
                    "owner": c.owner
                }
                for c in catalogs
            ]
        }
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
    
    async def _list_schemas(self, catalog: str) -> Sequence[TextContent]:
        """List all schemas in a catalog"""
        if not self.workspace_client:
            return [TextContent(type="text", text="Error: Databricks client not initialized")]
        
        schemas = list(self.workspace_client.schemas.list(catalog_name=catalog))
        result = {
            "catalog": catalog,
            "schemas": [
                {
                    "name": s.name,
                    "comment": s.comment,
                    "owner": s.owner
                }
                for s in schemas
            ]
        }
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
    
    async def _list_tables(self, catalog: str, schema: str) -> Sequence[TextContent]:
        """List all tables in a schema"""
        if not self.workspace_client:
            return [TextContent(type="text", text="Error: Databricks client not initialized")]
        
        tables = list(self.workspace_client.tables.list(
            catalog_name=catalog,
            schema_name=schema
        ))
        result = {
            "catalog": catalog,
            "schema": schema,
            "tables": [
                {
                    "name": t.name,
                    "table_type": t.table_type.value if t.table_type else None,
                    "comment": t.comment,
                    "owner": t.owner
                }
                for t in tables
            ]
        }
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
    
    async def _get_table_info(self, catalog: str, schema: str, table: str) -> Sequence[TextContent]:
        """Get detailed table information"""
        if not self.workspace_client:
            return [TextContent(type="text", text="Error: Databricks client not initialized")]
        
        table_info = self.workspace_client.tables.get(
            full_name=f"{catalog}.{schema}.{table}"
        )
        result = {
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
            "comment": table_info.comment,
            "properties": table_info.properties
        }
        return [TextContent(type="text", text=json.dumps(result, indent=2))]
    
    async def _execute_sql(self, query: str, warehouse_id: Optional[str] = None) -> Sequence[TextContent]:
        """Execute SQL query"""
        if not self.workspace_client:
            return [TextContent(type="text", text="Error: Databricks client not initialized")]
        
        try:
            # Get warehouse ID from environment if not provided
            if not warehouse_id:
                warehouse_id = os.getenv("DATABRICKS_WAREHOUSE_ID")
            
            if not warehouse_id:
                return [TextContent(
                    type="text",
                    text="Error: No warehouse_id provided and DATABRICKS_WAREHOUSE_ID not set"
                )]
            
            # Execute query using SQL execution API
            response = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=query,
                wait_timeout="30s"
            )
            
            # Convert result to pandas DataFrame
            if response.result and response.result.data_array:
                columns = [col.name for col in (response.manifest.schema.columns or [])]
                df = pd.DataFrame(response.result.data_array, columns=columns)
                
                result = {
                    "status": "success",
                    "row_count": len(df),
                    "columns": columns,
                    "data": df.to_dict(orient="records")
                }
            else:
                result = {
                    "status": "success",
                    "message": "Query executed successfully but returned no data"
                }
            
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
            
        except Exception as e:
            logger.error(f"Error executing SQL: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error executing query: {str(e)}")]
    
    async def _query_natural_language(
        self,
        question: str,
        catalog: str,
        schema: str,
        table: str,
        warehouse_id: Optional[str] = None
    ) -> Sequence[TextContent]:
        """Convert natural language to SQL and execute"""
        if not self.anthropic_client:
            return [TextContent(
                type="text",
                text="Error: Anthropic client not initialized. Set ANTHROPIC_API_KEY environment variable."
            )]
        
        try:
            # Get table schema
            table_info = self.workspace_client.tables.get(
                full_name=f"{catalog}.{schema}.{table}"
            )
            
            schema_text = "\n".join([
                f"- {col.name} ({col.type_text or col.type_name.value if col.type_name else 'unknown'}): {col.comment or 'No description'}"
                for col in (table_info.columns or [])
            ])
            
            # Generate SQL using Claude
            prompt = f"""Convert this natural language question to a SQL query for Databricks Delta Lake.

Table: {catalog}.{schema}.{table}
Description: {table_info.comment or 'No description'}

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
            
            # Execute the generated query
            result = await self._execute_sql(sql_query, warehouse_id)
            
            # Prepend the generated SQL
            return [
                TextContent(type="text", text=f"Generated SQL:\n{sql_query}\n\n"),
                *result
            ]
            
        except Exception as e:
            logger.error(f"Error in NL query: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error: {str(e)}")]
    
    async def _create_chart(
        self,
        query: str,
        chart_type: str,
        x_column: Optional[str],
        y_column: Optional[str],
        title: str,
        warehouse_id: Optional[str] = None
    ) -> Sequence[TextContent | ImageContent]:
        """Create a Plotly chart from query results"""
        try:
            # Execute query first
            result = await self._execute_sql(query, warehouse_id)
            result_text = result[0].text
            result_data = json.loads(result_text)
            
            if result_data.get("status") != "success" or "data" not in result_data:
                return result
            
            # Convert to DataFrame
            df = pd.DataFrame(result_data["data"])
            
            if df.empty:
                return [TextContent(type="text", text="No data to chart")]
            
            # Create chart based on type
            fig = None
            
            if chart_type == "bar":
                if x_column and y_column:
                    fig = px.bar(df, x=x_column, y=y_column, title=title)
                else:
                    # Auto-detect columns
                    fig = px.bar(df, x=df.columns[0], y=df.columns[1], title=title)
            
            elif chart_type == "line":
                if x_column and y_column:
                    fig = px.line(df, x=x_column, y=y_column, title=title)
                else:
                    fig = px.line(df, x=df.columns[0], y=df.columns[1], title=title)
            
            elif chart_type == "scatter":
                if x_column and y_column:
                    fig = px.scatter(df, x=x_column, y=y_column, title=title)
                else:
                    fig = px.scatter(df, x=df.columns[0], y=df.columns[1], title=title)
            
            elif chart_type == "pie":
                if x_column and y_column:
                    fig = px.pie(df, names=x_column, values=y_column, title=title)
                else:
                    fig = px.pie(df, names=df.columns[0], values=df.columns[1], title=title)
            
            elif chart_type == "histogram":
                col = x_column or df.columns[0]
                fig = px.histogram(df, x=col, title=title)
            
            elif chart_type == "box":
                col = y_column or df.columns[0]
                fig = px.box(df, y=col, title=title)
            
            if fig is None:
                return [TextContent(type="text", text=f"Unsupported chart type: {chart_type}")]
            
            # Convert to image
            img_bytes = fig.to_image(format="png", width=1200, height=800)
            import base64
            img_base64 = base64.b64encode(img_bytes).decode()
            
            return [
                TextContent(type="text", text=f"Chart created successfully ({chart_type})"),
                ImageContent(type="image", data=img_base64, mimeType="image/png")
            ]
            
        except Exception as e:
            logger.error(f"Error creating chart: {e}", exc_info=True)
            return [TextContent(type="text", text=f"Error creating chart: {str(e)}")]
    
    def initialize_clients(self):
        """Initialize Databricks and Anthropic clients"""
        try:
            # Initialize Databricks client
            config = Config(
                host=os.getenv("DATABRICKS_HOST"),
                token=os.getenv("DATABRICKS_TOKEN")
            )
            self.workspace_client = WorkspaceClient(config=config)
            logger.info("Databricks client initialized")
            
            # Initialize Anthropic client if API key is available
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if api_key:
                self.anthropic_client = Anthropic(api_key=api_key)
                logger.info("Anthropic client initialized")
            else:
                logger.warning("ANTHROPIC_API_KEY not set, NL queries will not work")
                
        except Exception as e:
            logger.error(f"Error initializing clients: {e}", exc_info=True)
            raise
    
    async def run(self):
        """Run the MCP server"""
        self.initialize_clients()
        
        async with stdio_server() as (read_stream, write_stream):
            await self.app.run(
                read_stream,
                write_stream,
                self.app.create_initialization_options()
            )


def main():
    """Main entry point"""
    import sys
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Create and run server
    server = DatabricksMCPServer()
    asyncio.run(server.run())


if __name__ == "__main__":
    main()

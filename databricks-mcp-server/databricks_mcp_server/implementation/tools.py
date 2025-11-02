"""
Tool handlers for Databricks MCP Server
Handles all MCP tool implementations for Unity Catalog operations
"""
import logging
import os
import base64
from typing import Optional, Sequence, List
import pandas as pd
import plotly.express as px
from anthropic import Anthropic
from databricks.sdk import WorkspaceClient
from mcp.types import Tool, TextContent, ImageContent, EmbeddedResource

from databricks_mcp_server.pydantic_models import (
    # Input models
    ListCatalogsInput,
    ListSchemasInput,
    ListTablesInput,
    GetTableInfoInput,
    ExecuteSQLInput,
    QueryNaturalLanguageInput,
    CreateChartInput,
    # Output models
    ListCatalogsOutput,
    CatalogInfo,
    ListSchemasOutput,
    SchemaInfo,
    ListTablesOutput,
    TableInfo,
    DetailedTableInfo,
    ColumnInfo,
    ExecuteSQLOutput,
    QueryNaturalLanguageOutput,
    CreateChartOutput,
    ErrorOutput,
    # Helper functions
    get_tool_input_schema,
    format_tool_output,
)

logger = logging.getLogger(__name__)


class ToolHandler:
    """Handles all MCP tool operations"""
    
    def __init__(
        self,
        workspace_client: Optional[WorkspaceClient] = None,
        anthropic_client: Optional[Anthropic] = None
    ):
        self.workspace_client = workspace_client
        self.anthropic_client = anthropic_client
    
    def set_workspace_client(self, client: WorkspaceClient):
        """Set the Databricks workspace client"""
        self.workspace_client = client
    
    def set_anthropic_client(self, client: Anthropic):
        """Set the Anthropic API client"""
        self.anthropic_client = client
    
    def get_tool_definitions(self) -> List[Tool]:
        """Get list of available tools with their schemas"""
        return [
            Tool(
                name="list_catalogs",
                description="List all catalogs in Unity Catalog",
                inputSchema=get_tool_input_schema(ListCatalogsInput)
            ),
            Tool(
                name="list_schemas",
                description="List all schemas in a catalog",
                inputSchema=get_tool_input_schema(ListSchemasInput)
            ),
            Tool(
                name="list_tables",
                description="List all tables in a schema",
                inputSchema=get_tool_input_schema(ListTablesInput)
            ),
            Tool(
                name="get_table_info",
                description="Get detailed information about a table",
                inputSchema=get_tool_input_schema(GetTableInfoInput)
            ),
            Tool(
                name="execute_sql",
                description="Execute a SQL query on Databricks",
                inputSchema=get_tool_input_schema(ExecuteSQLInput)
            ),
            Tool(
                name="query_natural_language",
                description="Convert natural language to SQL and execute",
                inputSchema=get_tool_input_schema(QueryNaturalLanguageInput)
            ),
            Tool(
                name="create_chart",
                description="Create a Plotly chart from query results",
                inputSchema=get_tool_input_schema(CreateChartInput)
            )
        ]
    
    # ========================================================================
    # Tool Implementations
    # ========================================================================
    
    async def list_catalogs(self, input_data: ListCatalogsInput) -> Sequence[TextContent]:
        """List all Unity Catalogs"""
        if not self.workspace_client:
            error = ErrorOutput(error="Databricks client not initialized")
            return [TextContent(type="text", text=format_tool_output(error))]
        
        catalogs = list(self.workspace_client.catalogs.list())
        output = ListCatalogsOutput(
            catalogs=[
                CatalogInfo(
                    name=c.name,
                    comment=c.comment,
                    owner=c.owner
                )
                for c in catalogs
            ]
        )
        return [TextContent(type="text", text=format_tool_output(output))]
    
    async def list_schemas(self, input_data: ListSchemasInput) -> Sequence[TextContent]:
        """List all schemas in a catalog"""
        if not self.workspace_client:
            error = ErrorOutput(error="Databricks client not initialized")
            return [TextContent(type="text", text=format_tool_output(error))]
        
        schemas = list(self.workspace_client.schemas.list(catalog_name=input_data.catalog))
        output = ListSchemasOutput(
            catalog=input_data.catalog,
            schemas=[
                SchemaInfo(
                    name=s.name,
                    comment=s.comment,
                    owner=s.owner
                )
                for s in schemas
            ]
        )
        return [TextContent(type="text", text=format_tool_output(output))]
    
    async def list_tables(self, input_data: ListTablesInput) -> Sequence[TextContent]:
        """List all tables in a schema"""
        if not self.workspace_client:
            error = ErrorOutput(error="Databricks client not initialized")
            return [TextContent(type="text", text=format_tool_output(error))]
        
        tables = list(self.workspace_client.tables.list(
            catalog_name=input_data.catalog,
            schema_name=input_data.schema
        ))
        output = ListTablesOutput(
            catalog=input_data.catalog,
            schema=input_data.schema,
            tables=[
                TableInfo(
                    name=t.name,
                    table_type=t.table_type.value if t.table_type else None,
                    comment=t.comment,
                    owner=t.owner
                )
                for t in tables
            ]
        )
        return [TextContent(type="text", text=format_tool_output(output))]
    
    async def get_table_info(self, input_data: GetTableInfoInput) -> Sequence[TextContent]:
        """Get detailed table information"""
        if not self.workspace_client:
            error = ErrorOutput(error="Databricks client not initialized")
            return [TextContent(type="text", text=format_tool_output(error))]
        
        table_info = self.workspace_client.tables.get(
            full_name=f"{input_data.catalog}.{input_data.schema}.{input_data.table}"
        )
        output = DetailedTableInfo(
            name=table_info.name,
            catalog_name=table_info.catalog_name,
            schema_name=table_info.schema_name,
            table_type=table_info.table_type.value if table_info.table_type else None,
            data_source_format=table_info.data_source_format.value if table_info.data_source_format else None,
            columns=[
                ColumnInfo(
                    name=col.name,
                    type_name=col.type_name.value if col.type_name else None,
                    type_text=col.type_text,
                    comment=col.comment,
                    nullable=col.nullable,
                    position=col.position
                )
                for col in (table_info.columns or [])
            ],
            owner=table_info.owner,
            comment=table_info.comment,
            properties=table_info.properties
        )
        return [TextContent(type="text", text=format_tool_output(output))]
    
    async def execute_sql(self, input_data: ExecuteSQLInput) -> Sequence[TextContent]:
        """Execute SQL query"""
        if not self.workspace_client:
            error = ErrorOutput(error="Databricks client not initialized")
            return [TextContent(type="text", text=format_tool_output(error))]
        
        try:
            # Get warehouse ID
            warehouse_id = input_data.warehouse_id or os.getenv("DATABRICKS_WAREHOUSE_ID")
            
            if not warehouse_id:
                error = ErrorOutput(
                    error="No warehouse_id provided and DATABRICKS_WAREHOUSE_ID not set"
                )
                return [TextContent(type="text", text=format_tool_output(error))]
            
            # Execute query
            response = self.workspace_client.statement_execution.execute_statement(
                warehouse_id=warehouse_id,
                statement=input_data.query,
                wait_timeout="30s"
            )
            
            # Convert result to pandas DataFrame
            if response.result and response.result.data_array:
                columns = [col.name for col in (response.manifest.schema.columns or [])]
                df = pd.DataFrame(response.result.data_array, columns=columns)
                
                output = ExecuteSQLOutput(
                    status="success",
                    row_count=len(df),
                    columns=columns,
                    data=df.to_dict(orient="records")
                )
            else:
                output = ExecuteSQLOutput(
                    status="success",
                    message="Query executed successfully but returned no data"
                )
            
            return [TextContent(type="text", text=format_tool_output(output))]
            
        except Exception as e:
            logger.error(f"Error executing SQL: {e}", exc_info=True)
            error = ErrorOutput(error=f"Error executing query: {str(e)}")
            return [TextContent(type="text", text=format_tool_output(error))]
    
    async def query_natural_language(
        self,
        input_data: QueryNaturalLanguageInput
    ) -> Sequence[TextContent]:
        """Convert natural language to SQL and execute"""
        if not self.anthropic_client:
            error = ErrorOutput(
                error="Anthropic client not initialized. Set ANTHROPIC_API_KEY environment variable."
            )
            return [TextContent(type="text", text=format_tool_output(error))]
        
        try:
            # Get table schema
            table_info = self.workspace_client.tables.get(
                full_name=f"{input_data.catalog}.{input_data.schema}.{input_data.table}"
            )
            
            schema_text = "\n".join([
                f"- {col.name} ({col.type_text or col.type_name.value if col.type_name else 'unknown'}): {col.comment or 'No description'}"
                for col in (table_info.columns or [])
            ])
            
            # Generate SQL using Claude
            prompt = f"""Convert this natural language question to a SQL query for Databricks Delta Lake.

Table: {input_data.catalog}.{input_data.schema}.{input_data.table}
Description: {table_info.comment or 'No description'}

Schema:
{schema_text}

Question: {input_data.question}

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
            exec_input = ExecuteSQLInput(
                query=sql_query,
                warehouse_id=input_data.warehouse_id
            )
            exec_result = await self.execute_sql(exec_input)
            
            # Parse execution result
            exec_output = ExecuteSQLOutput.model_validate_json(exec_result[0].text)
            
            # Create combined output
            output = QueryNaturalLanguageOutput(
                generated_sql=sql_query,
                execution_result=exec_output
            )
            
            return [TextContent(type="text", text=format_tool_output(output))]
            
        except Exception as e:
            logger.error(f"Error in NL query: {e}", exc_info=True)
            error = ErrorOutput(error=str(e))
            return [TextContent(type="text", text=format_tool_output(error))]
    
    async def create_chart(
        self,
        input_data: CreateChartInput
    ) -> Sequence[TextContent | ImageContent]:
        """Create a Plotly chart from query results"""
        try:
            # Execute query first
            exec_input = ExecuteSQLInput(
                query=input_data.query,
                warehouse_id=input_data.warehouse_id
            )
            result = await self.execute_sql(exec_input)
            
            # Parse result
            exec_output = ExecuteSQLOutput.model_validate_json(result[0].text)
            
            if exec_output.status != "success" or not exec_output.data:
                error = ErrorOutput(error="No data to chart")
                return [TextContent(type="text", text=format_tool_output(error))]
            
            # Convert to DataFrame
            df = pd.DataFrame(exec_output.data)
            
            if df.empty:
                error = ErrorOutput(error="No data to chart")
                return [TextContent(type="text", text=format_tool_output(error))]
            
            # Create chart based on type
            fig = self._create_plotly_figure(df, input_data)
            
            if fig is None:
                error = ErrorOutput(error=f"Unsupported chart type: {input_data.chart_type.value}")
                return [TextContent(type="text", text=format_tool_output(error))]
            
            # Convert to image
            img_bytes = fig.to_image(format="png", width=1200, height=800)
            img_base64 = base64.b64encode(img_bytes).decode()
            
            # Create output
            output = CreateChartOutput(
                status="success",
                message=f"Chart created successfully ({input_data.chart_type.value})",
                chart_type=input_data.chart_type.value,
                image_data=img_base64,
                mime_type="image/png"
            )
            
            return [
                TextContent(type="text", text=format_tool_output(output)),
                ImageContent(type="image", data=img_base64, mimeType="image/png")
            ]
            
        except Exception as e:
            logger.error(f"Error creating chart: {e}", exc_info=True)
            error = ErrorOutput(error=f"Error creating chart: {str(e)}")
            return [TextContent(type="text", text=format_tool_output(error))]
    
    def _create_plotly_figure(self, df: pd.DataFrame, input_data: CreateChartInput):
        """Create a Plotly figure based on chart type and input"""
        chart_type = input_data.chart_type.value
        x_col = input_data.x_column or df.columns[0]
        y_col = input_data.y_column or (df.columns[1] if len(df.columns) > 1 else df.columns[0])
        
        if chart_type == "bar":
            return px.bar(df, x=x_col, y=y_col, title=input_data.title)
        elif chart_type == "line":
            return px.line(df, x=x_col, y=y_col, title=input_data.title)
        elif chart_type == "scatter":
            return px.scatter(df, x=x_col, y=y_col, title=input_data.title)
        elif chart_type == "pie":
            return px.pie(df, names=x_col, values=y_col, title=input_data.title)
        elif chart_type == "histogram":
            return px.histogram(df, x=x_col, title=input_data.title)
        elif chart_type == "box":
            return px.box(df, y=y_col, title=input_data.title)
        
        return None
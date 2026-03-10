"""
Python MCP Client for Databricks Unity Catalog
==============================================

This module defines a high-level asynchronous client that connects
to a Databricks MCP (Model Context Protocol) server instance using
the stdio transport. The client allows Python applications to
programmatically interact with the MCP server's tools, resources,
and prompts — such as listing catalogs, schemas, executing SQL,
and generating charts.

Author: Bennie Haelen
Date: 2025
"""

import os
import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabricksMCPClient:
    """
    High-level Python client for the Databricks MCP Server.
    
    Provides convenience methods to call MCP tools and access
    resources or prompts, abstracting away lower-level session
    management and I/O details.
    """

    def __init__(self, server_script_path: str = "databricks-mcp-server"):
        """
        Initialize the MCP client instance.

        Args:
            server_script_path: Path to the MCP server executable or module name.
                               Default assumes an importable module.
        """
        self.server_script_path = server_script_path
        self.session: Optional[ClientSession] = None
        self._read_stream = None
        self._write_stream = None
        self._server_params = None

    # -----------------------------------------------------------------------
    # Connection Management
    # -----------------------------------------------------------------------
    @asynccontextmanager
    async def connect(self):
        """
        Async context manager for connecting to the MCP server.

        Creates a subprocess running the Databricks MCP server via stdio
        transport, then initializes an MCP session over it.

        Usage:
            async with DatabricksMCPClient().connect() as client:
                await client.list_catalogs()
        """
        # Define parameters for launching the server subprocess
        server_params = StdioServerParameters(
            command="python",
            args=["-m", "databricks_mcp_server.server"],
            env=os.environ.copy()
        )

        # Open stdio connection to MCP server and establish session
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()  # perform MCP handshake
                self.session = session
                yield self
                # When context exits, automatically close session
                self.session = None

    # -----------------------------------------------------------------------
    # TOOL CALLS
    # -----------------------------------------------------------------------
    async def list_catalogs(self) -> Dict[str, Any]:
        """List all catalogs available in Databricks Unity Catalog."""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        result = await self.session.call_tool("list_catalogs", {})
        return json.loads(result.content[0].text)

    async def list_schemas(self, catalog: str) -> Dict[str, Any]:
        """List all schemas within a specified catalog."""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        result = await self.session.call_tool("list_schemas", {"catalog": catalog})
        return json.loads(result.content[0].text)

    async def list_tables(self, catalog: str, schema: str) -> Dict[str, Any]:
        """List all tables under a given catalog and schema."""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        result = await self.session.call_tool(
            "list_tables",
            {"catalog": catalog, "schema_name": schema}
        )
        return json.loads(result.content[0].text)

    async def get_table_info(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Retrieve detailed metadata about a specific Unity Catalog table."""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        result = await self.session.call_tool(
            "get_table_info",
            {"catalog": catalog, "schema_name": schema, "table": table}
        )
        return json.loads(result.content[0].text)

    async def execute_sql(self, query: str, warehouse_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Execute a SQL query using Databricks SQL Warehouse.

        Args:
            query: SQL query string to execute.
            warehouse_id: Optional warehouse identifier to use.
        """
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        args = {"query": query}
        if warehouse_id:
            args["warehouse_id"] = warehouse_id

        result = await self.session.call_tool("execute_sql", args)
        return json.loads(result.content[0].text)

    async def query_natural_language(
        self,
        question: str,
        catalog: str,
        schema: str,
        table: str,
        warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Convert a natural language question into SQL and execute it.

        Args:
            question: Natural language question to be answered.
            catalog: Databricks catalog containing the data.
            schema: Databricks schema containing the data.
            table: Optional table name to query.
            warehouse_id: Optional warehouse ID for query execution.

        Returns:
            A dictionary containing the generated SQL and query results.
        """
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        args = {
            "question": question,
            "catalog": catalog,
            "schema_name": schema,
            "table": table
        }
        if warehouse_id:
            args["warehouse_id"] = warehouse_id

        result = await self.session.call_tool("query_natural_language", args)

        # Collect concatenated text from response
        full_text = "".join([c.text for c in result.content if hasattr(c, "text")])
        return {"response": full_text}

    async def create_chart(
        self,
        query: str,
        chart_type: str,
        x_column: Optional[str] = None,
        y_column: Optional[str] = None,
        title: str = "Chart",
        warehouse_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a chart visualization from query results.

        Args:
            query: SQL query to execute for chart data.
            chart_type: Plotly chart type ("bar", "line", "pie", etc.).
            x_column: Column for X-axis or categories.
            y_column: Column for Y-axis values.
            title: Optional chart title.
            warehouse_id: Optional SQL warehouse for query execution.

        Returns:
            A dictionary containing chart metadata and optional image data.
        """
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        args = {"query": query, "chart_type": chart_type, "title": title}
        if x_column:
            args["x_column"] = x_column
        if y_column:
            args["y_column"] = y_column
        if warehouse_id:
            args["warehouse_id"] = warehouse_id

        result = await self.session.call_tool("create_chart", args)

        # Parse response and include any embedded image data
        response = {"message": result.content[0].text if result.content else ""}
        for content in result.content:
            if hasattr(content, "data") and hasattr(content, "mimeType"):
                response["image_data"] = content.data
                response["mime_type"] = content.mimeType

        return response

    # -----------------------------------------------------------------------
    # RESOURCE & PROMPT ACCESS
    # -----------------------------------------------------------------------
    async def list_resources(self) -> List[Dict[str, Any]]:
        """Retrieve a list of all resources exposed by the MCP server."""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        resources = await self.session.list_resources()
        return [
            {
                "uri": r.uri,
                "name": r.name,
                "description": r.description,
                "mimeType": r.mimeType,
            }
            for r in resources.resources
        ]

    async def read_resource(self, uri: str) -> Dict[str, Any]:
        """Read a resource’s contents by URI."""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        resource = await self.session.read_resource(uri)
        return json.loads(resource.contents[0].text)

    async def list_prompts(self) -> List[Dict[str, Any]]:
        """List all available prompt templates exposed by the MCP server."""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        prompts = await self.session.list_prompts()
        return [
            {
                "name": p.name,
                "description": p.description,
                "arguments": p.arguments,
            }
            for p in prompts.prompts
        ]

    async def get_prompt(
        self, name: str, arguments: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Retrieve the content and metadata of a specific prompt.

        Args:
            name: Prompt name as registered on the MCP server.
            arguments: Optional mapping for parameter substitution.
        """
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        prompt = await self.session.get_prompt(name, arguments)
        return {
            "description": prompt.description,
            "messages": [
                {
                    "role": msg.role,
                    "content": msg.content.text
                    if hasattr(msg.content, "text")
                    else str(msg.content),
                }
                for msg in prompt.messages
            ],
        }


# ---------------------------------------------------------------------------
# Example CLI usage for quick testing
# ---------------------------------------------------------------------------
async def example_usage():
    """Demonstrates how to connect and interact with the MCP server."""
    client = DatabricksMCPClient()

    async with client.connect():
        print("Connected to Databricks MCP Server\n")

        # Example: List catalogs
        print("=== Catalogs ===")
        catalogs = await client.list_catalogs()
        print(json.dumps(catalogs, indent=2))

        # Example: List resources
        print("\n=== Resources ===")
        resources = await client.list_resources()
        for resource in resources:
            print(f"- {resource['name']}: {resource['uri']}")

        # Example: List available prompts
        print("\n=== Available Prompts ===")
        prompts = await client.list_prompts()
        for prompt in prompts:
            print(f"- {prompt['name']}: {prompt['description']}")

        # Uncomment the examples below when you have a SQL warehouse configured
        # print("\n=== SQL Query ===")
        # result = await client.execute_sql("SELECT * FROM main.default.my_table LIMIT 10")
        # print(json.dumps(result, indent=2))

        # print("\n=== Natural Language Query ===")
        # result = await client.query_natural_language(
        #     "What are the top 5 records?",
        #     "main",
        #     "default",
        #     "my_table"
        # )
        # print(result['response'])


# Run example when executed directly
if __name__ == "__main__":
    asyncio.run(example_usage())

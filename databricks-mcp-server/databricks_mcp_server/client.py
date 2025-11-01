"""
Python MCP Client for Databricks Unity Catalog
Connects to the MCP server and provides a high-level interface
"""
import os
import asyncio
import json
import logging
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabricksMCPClient:
    """Python client for the Databricks MCP Server"""
    
    def __init__(self, server_script_path: str = "databricks-mcp-server"):
        """
        Initialize the MCP client
        
        Args:
            server_script_path: Path to the server script or command
        """
        self.server_script_path = server_script_path
        self.session: Optional[ClientSession] = None
        self._read_stream = None
        self._write_stream = None
        self._server_params = None
    
    @asynccontextmanager
    async def connect(self):
        """Context manager to connect to the MCP server"""
        server_params = StdioServerParameters(
            command="python",
            args=["-m", "databricks_mcp_server.server"],
            env=os.environ.copy()
        )
        
        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                self.session = session
                yield self
                self.session = None
    
    async def list_catalogs(self) -> Dict[str, Any]:
        """List all Unity Catalogs"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        result = await self.session.call_tool("list_catalogs", {})
        return json.loads(result.content[0].text)
    
    async def list_schemas(self, catalog: str) -> Dict[str, Any]:
        """List all schemas in a catalog"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        result = await self.session.call_tool("list_schemas", {"catalog": catalog})
        return json.loads(result.content[0].text)
    
    async def list_tables(self, catalog: str, schema: str) -> Dict[str, Any]:
        """List all tables in a schema"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        result = await self.session.call_tool("list_tables", {
            "catalog": catalog,
            "schema": schema
        })
        return json.loads(result.content[0].text)
    
    async def get_table_info(self, catalog: str, schema: str, table: str) -> Dict[str, Any]:
        """Get detailed information about a table"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        result = await self.session.call_tool("get_table_info", {
            "catalog": catalog,
            "schema": schema,
            "table": table
        })
        return json.loads(result.content[0].text)
    
    async def execute_sql(self, query: str, warehouse_id: Optional[str] = None) -> Dict[str, Any]:
        """Execute a SQL query"""
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
        """Ask a natural language question and get results"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        args = {
            "question": question,
            "catalog": catalog,
            "schema": schema,
            "table": table
        }
        if warehouse_id:
            args["warehouse_id"] = warehouse_id
        
        result = await self.session.call_tool("query_natural_language", args)
        
        # Return both the SQL and the results
        full_text = "".join([c.text for c in result.content if hasattr(c, 'text')])
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
        """Create a chart from query results"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        args = {
            "query": query,
            "chart_type": chart_type,
            "title": title
        }
        if x_column:
            args["x_column"] = x_column
        if y_column:
            args["y_column"] = y_column
        if warehouse_id:
            args["warehouse_id"] = warehouse_id
        
        result = await self.session.call_tool("create_chart", args)
        
        # Extract image data if present
        response = {"message": result.content[0].text if result.content else ""}
        
        for content in result.content:
            if hasattr(content, 'data') and hasattr(content, 'mimeType'):
                response["image_data"] = content.data
                response["mime_type"] = content.mimeType
        
        return response
    
    async def list_resources(self) -> List[Dict[str, Any]]:
        """List available resources"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        resources = await self.session.list_resources()
        return [
            {
                "uri": r.uri,
                "name": r.name,
                "description": r.description,
                "mimeType": r.mimeType
            }
            for r in resources.resources
        ]
    
    async def read_resource(self, uri: str) -> Dict[str, Any]:
        """Read a specific resource"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        resource = await self.session.read_resource(uri)
        return json.loads(resource.contents[0].text)
    
    async def list_prompts(self) -> List[Dict[str, Any]]:
        """List available prompt templates"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        prompts = await self.session.list_prompts()
        return [
            {
                "name": p.name,
                "description": p.description,
                "arguments": p.arguments
            }
            for p in prompts.prompts
        ]
    
    async def get_prompt(self, name: str, arguments: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Get a specific prompt"""
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")
        
        prompt = await self.session.get_prompt(name, arguments)
        return {
            "description": prompt.description,
            "messages": [
                {
                    "role": msg.role,
                    "content": msg.content.text if hasattr(msg.content, 'text') else str(msg.content)
                }
                for msg in prompt.messages
            ]
        }


async def example_usage():
    """Example usage of the client"""
    client = DatabricksMCPClient()
    
    async with client.connect():
        print("Connected to Databricks MCP Server\n")
        
        # List catalogs
        print("=== Catalogs ===")
        catalogs = await client.list_catalogs()
        print(json.dumps(catalogs, indent=2))
        
        # List resources
        print("\n=== Resources ===")
        resources = await client.list_resources()
        for resource in resources:
            print(f"- {resource['name']}: {resource['uri']}")
        
        # List prompts
        print("\n=== Available Prompts ===")
        prompts = await client.list_prompts()
        for prompt in prompts:
            print(f"- {prompt['name']}: {prompt['description']}")
        
        # Example SQL query (uncomment when you have a warehouse configured)
        # print("\n=== SQL Query ===")
        # result = await client.execute_sql("SELECT * FROM main.default.my_table LIMIT 10")
        # print(json.dumps(result, indent=2))
        
        # Example natural language query
        # print("\n=== Natural Language Query ===")
        # result = await client.query_natural_language(
        #     "What are the top 5 records?",
        #     "main",
        #     "default",
        #     "my_table"
        # )
        # print(result['response'])


if __name__ == "__main__":
    asyncio.run(example_usage())

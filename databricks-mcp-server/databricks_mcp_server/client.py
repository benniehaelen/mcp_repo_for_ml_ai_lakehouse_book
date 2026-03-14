"""
Python MCP Client for Databricks Unity Catalog
==============================================

This module defines a high-level asynchronous client that connects
to a Databricks MCP (Model Context Protocol) server instance using
the stdio transport. The client dynamically discovers available tools
at connect time and allows invoking them by name — no hard-coded
tool methods required.

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

    Dynamically discovers available tools on connect and provides
    a generic call_tool() method as well as attribute-style access
    (e.g. client.list_catalogs()) for convenience.
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
        self.available_tools: Dict[str, Dict[str, Any]] = {}

    # -----------------------------------------------------------------------
    # Connection Management
    # -----------------------------------------------------------------------
    @asynccontextmanager
    async def connect(self):
        """
        Async context manager for connecting to the MCP server.

        Creates a subprocess running the Databricks MCP server via stdio
        transport, then initializes an MCP session and discovers available
        tools.

        Usage:
            async with DatabricksMCPClient().connect() as client:
                tools = client.list_tools()
                result = await client.call_tool("list_catalogs")
        """
        server_params = StdioServerParameters(
            command="python",
            args=["-m", "databricks_mcp_server.server"],
            env=os.environ.copy()
        )

        async with stdio_client(server_params) as (read, write):
            async with ClientSession(read, write) as session:
                await session.initialize()
                self.session = session

                # Discover all tools exposed by the server
                await self._discover_tools()
                logger.info(
                    f"Discovered {len(self.available_tools)} tools: "
                    f"{', '.join(self.available_tools.keys())}"
                )

                yield self
                self.session = None
                self.available_tools = {}

    async def _discover_tools(self):
        """Fetch the tool list from the server and cache their definitions."""
        tools_response = await self.session.list_tools()
        self.available_tools = {
            tool.name: {
                "description": tool.description,
                "inputSchema": tool.inputSchema,
            }
            for tool in tools_response.tools
        }

    # -----------------------------------------------------------------------
    # Dynamic tool access via attribute syntax
    # -----------------------------------------------------------------------
    def __getattr__(self, name: str):
        """
        Allow attribute-style tool invocation, e.g. client.list_catalogs(**kwargs).

        If 'name' matches a discovered tool, returns an async callable that
        forwards keyword arguments to call_tool().
        """
        if name in self.available_tools:
            async def _dynamic_tool_call(**kwargs):
                return await self.call_tool(name, kwargs if kwargs else {})
            return _dynamic_tool_call
        raise AttributeError(f"'{type(self).__name__}' has no attribute '{name}'")

    # -----------------------------------------------------------------------
    # TOOL CALLS
    # -----------------------------------------------------------------------
    def list_tools(self) -> List[Dict[str, Any]]:
        """Return the list of tools discovered from the server."""
        return [
            {"name": name, **info}
            for name, info in self.available_tools.items()
        ]

    async def call_tool(self, name: str, arguments: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Invoke any MCP tool by name with the given arguments.

        Args:
            name: Tool name as registered on the server.
            arguments: Dictionary of tool input parameters.

        Returns:
            Parsed JSON response from the tool, or a dict with
            'response' / 'message' keys for non-JSON content.
        """
        if not self.session:
            raise RuntimeError("Not connected. Use 'async with client.connect():'")

        if name not in self.available_tools:
            available = ", ".join(self.available_tools.keys()) or "(none)"
            raise ValueError(
                f"Unknown tool '{name}'. Available tools: {available}"
            )

        result = await self.session.call_tool(name, arguments or {})

        return self._parse_tool_result(result)

    @staticmethod
    def _parse_tool_result(result) -> Dict[str, Any]:
        """
        Parse an MCP tool result into a dictionary.

        Handles JSON text responses, multi-part text, and embedded
        image content.
        """
        if not result.content:
            return {}

        # Check for embedded image data (e.g. from create_chart)
        response = {}
        has_image = False
        for content in result.content:
            if hasattr(content, "data") and hasattr(content, "mimeType"):
                response["image_data"] = content.data
                response["mime_type"] = content.mimeType
                has_image = True

        # Try to parse the first text content as JSON
        text_parts = [c.text for c in result.content if hasattr(c, "text")]
        if text_parts:
            full_text = "".join(text_parts)
            try:
                parsed = json.loads(full_text)
                if has_image:
                    response["message"] = full_text
                    return response
                return parsed
            except json.JSONDecodeError:
                response["response"] = full_text
                return response

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
        """Read a resource's contents by URI."""
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

        # Show dynamically discovered tools
        print("=== Discovered Tools ===")
        for tool in client.list_tools():
            print(f"- {tool['name']}: {tool['description']}")

        # Example: Call tools dynamically via call_tool()
        print("\n=== Catalogs (via call_tool) ===")
        catalogs = await client.call_tool("list_catalogs")
        print(json.dumps(catalogs, indent=2))

        # Example: Call tools via attribute-style access
        print("\n=== Catalogs (via attribute access) ===")
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


# Run example when executed directly
if __name__ == "__main__":
    asyncio.run(example_usage())

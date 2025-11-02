"""
Databricks Unity Catalog MCP Server
===================================

Provides tools, resources, and prompts for querying Databricks Unity Catalog.

Architecture Overview
---------------------
This modular design separates implementation logic into three core modules:
  - **resources.py** → Handles Unity Catalog resources (catalogs, schemas, tables)
  - **prompts.py**   → Defines and retrieves prompt templates
  - **tools.py**     → Executes tool actions such as SQL queries and chart creation

Responsibilities
----------------
- Initializes and runs an MCP server compatible with Databricks Unity Catalog
- Defines handlers for MCP protocol methods: list_resources, list_tools, etc.
- Integrates Databricks WorkspaceClient and Anthropic LLM client
- Validates inputs using Pydantic models
- Returns standardized output via MCP SDK content types

Environment Variables
---------------------
- `DATABRICKS_HOST`          → Databricks workspace URL
- `DATABRICKS_TOKEN`         → Databricks PAT token
- `ANTHROPIC_API_KEY`        → (Optional) Anthropic API key for natural language querying

Author: Bennie Haelen
Date: 2025
"""

import os
import asyncio
import logging
from typing import Any, Optional, Sequence

# External dependencies
from anthropic import Anthropic
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from pydantic import ValidationError

# MCP protocol imports
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import (
    Resource,
    Tool,
    TextContent,
    ImageContent,
    EmbeddedResource,
    Prompt,
    GetPromptResult,
    ResourceTemplate,
)

# Implementation imports
from databricks_mcp_server.implementation import (
    ResourceHandler,
    PromptHandler,
    ToolHandler,
)

# Pydantic models and utilities
from databricks_mcp_server.pydantic_models import (
    # Input models
    ListCatalogsInput,
    ListSchemasInput,
    ListTablesInput,
    GetTableInfoInput,
    ExecuteSQLInput,
    QueryNaturalLanguageInput,
    CreateChartInput,
    # Helper functions
    parse_tool_input,
    format_tool_output,
    # Error model
    ErrorOutput,
)

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabricksMCPServer:
    """
    Main Databricks MCP Server class
    --------------------------------
    Implements the MCP protocol for Databricks Unity Catalog by wiring together:
      - ResourceHandler  (resources)
      - PromptHandler    (prompt templates)
      - ToolHandler      (tools and operations)

    Methods
    -------
    - _setup_handlers(): registers all MCP endpoints
    - initialize_clients(): initializes Databricks and Anthropic clients
    - run(): starts the MCP server event loop
    """

    def __init__(self):
        """Initialize the Databricks MCP Server and its handlers."""
        self.app = Server("databricks-mcp-server")

        # Core clients (initialized later)
        self.workspace_client: Optional[WorkspaceClient] = None
        self.anthropic_client: Optional[Anthropic] = None

        # Implementation layer handlers
        self.resource_handler = ResourceHandler()
        self.prompt_handler = PromptHandler()
        self.tool_handler = ToolHandler()

        # Register MCP handlers for resources, prompts, and tools
        self._setup_handlers()

    # -----------------------------------------------------------------------
    # Handler Setup
    # -----------------------------------------------------------------------
    def _setup_handlers(self):
        """
        Registers all MCP protocol handlers for resources, prompts, and tools.
        Each decorator binds an MCP operation to a handler method.
        """

        # -------------------------------------------------------------------
        # RESOURCE HANDLERS
        # -------------------------------------------------------------------

        @self.app.list_resources()
        async def list_resources() -> list[Resource]:
            """List all Unity Catalog resources."""
            return await self.resource_handler.list_resources()

        @self.app.read_resource()
        async def read_resource(uri: str) -> str:
            """
            Read a Unity Catalog resource by its URI.

            Args:
                uri: Resource URI (e.g., databricks://table/catalog/schema/table)
            Returns:
                Resource content as string
            """
            return await self.resource_handler.read_resource(uri)

        @self.app.list_resource_templates()
        async def list_resource_templates() -> list[ResourceTemplate]:
            """Return all supported resource templates."""
            return [
                ResourceTemplate(
                    uriTemplate="databricks://catalog/{catalog_name}",
                    name="Catalog Schemas",
                    description="Get schemas for any catalog by name",
                    mimeType="application/json",
                ),
                ResourceTemplate(
                    uriTemplate="databricks://table/{catalog_name}/{schema_name}/{table_name}",
                    name="Table Information",
                    description="Get detailed information about any table",
                    mimeType="application/json",
                ),
            ]

        # -------------------------------------------------------------------
        # PROMPT HANDLERS
        # -------------------------------------------------------------------

        @self.app.list_prompts()
        async def list_prompts() -> list[Prompt]:
            """List all available prompt templates."""
            return await self.prompt_handler.list_prompts()

        @self.app.get_prompt()
        async def get_prompt(name: str, arguments: dict[str, str] | None) -> GetPromptResult:
            """
            Retrieve a prompt template by name.

            Args:
                name: The name of the prompt
                arguments: Optional argument dict for template substitution
            """
            return await self.prompt_handler.get_prompt(name, arguments)

        # -------------------------------------------------------------------
        # TOOL HANDLERS
        # -------------------------------------------------------------------

        @self.app.list_tools()
        async def list_tools() -> list[Tool]:
            """Return all available tool definitions."""
            return self.tool_handler.get_tool_definitions()

        @self.app.call_tool()
        async def call_tool(
            name: str,
            arguments: Any,
        ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
            """
            Execute an MCP tool by name, validating inputs with Pydantic.

            Args:
                name: Tool name (e.g., 'list_catalogs', 'execute_sql', etc.)
                arguments: Tool input parameters (dict or None)
            Returns:
                Sequence of MCP content objects (TextContent, ImageContent, etc.)
            """
            try:
                # Dispatch to the correct tool handler based on name
                if name == "list_catalogs":
                    input_data = parse_tool_input(ListCatalogsInput, arguments or {})
                    return await self.tool_handler.list_catalogs(input_data)

                elif name == "list_schemas":
                    input_data = parse_tool_input(ListSchemasInput, arguments)
                    return await self.tool_handler.list_schemas(input_data)

                elif name == "list_tables":
                    input_data = parse_tool_input(ListTablesInput, arguments)
                    return await self.tool_handler.list_tables(input_data)

                elif name == "get_table_info":
                    input_data = parse_tool_input(GetTableInfoInput, arguments)
                    return await self.tool_handler.get_table_info(input_data)

                elif name == "execute_sql":
                    input_data = parse_tool_input(ExecuteSQLInput, arguments)
                    return await self.tool_handler.execute_sql(input_data)

                elif name == "query_natural_language":
                    input_data = parse_tool_input(QueryNaturalLanguageInput, arguments)
                    return await self.tool_handler.query_natural_language(input_data)

                elif name == "create_chart":
                    input_data = parse_tool_input(CreateChartInput, arguments)
                    return await self.tool_handler.create_chart(input_data)

                # Handle unknown tool
                else:
                    error = ErrorOutput(error=f"Unknown tool: {name}")
                    return [TextContent(type="text", text=format_tool_output(error))]

            except ValidationError as e:
                # Input validation failure
                error = ErrorOutput(error="Validation error", details=str(e))
                return [TextContent(type="text", text=format_tool_output(error))]

            except Exception as e:
                # General execution error
                logger.error(f"Error executing tool '{name}': {e}", exc_info=True)
                error = ErrorOutput(error=str(e))
                return [TextContent(type="text", text=format_tool_output(error))]

    # -----------------------------------------------------------------------
    # Client Initialization
    # -----------------------------------------------------------------------
    def initialize_clients(self):
        """
        Initialize Databricks and Anthropic clients using environment variables.
        Links clients to internal handlers.
        """
        try:
            # Create Databricks client configuration
            config = Config(
                host=os.getenv("DATABRICKS_HOST"),
                token=os.getenv("DATABRICKS_TOKEN"),
            )
            self.workspace_client = WorkspaceClient(config=config)
            logger.info("✓ Databricks client initialized")

            # Propagate Databricks client to handlers
            self.resource_handler.set_workspace_client(self.workspace_client)
            self.tool_handler.set_workspace_client(self.workspace_client)

            # Initialize Anthropic client if API key exists
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if api_key:
                self.anthropic_client = Anthropic(api_key=api_key)
                self.tool_handler.set_anthropic_client(self.anthropic_client)
                logger.info("✓ Anthropic client initialized")
            else:
                logger.warning("⚠ ANTHROPIC_API_KEY not set — NL queries disabled")

        except Exception as e:
            logger.error(f"✗ Error initializing clients: {e}", exc_info=True)
            raise

    # -----------------------------------------------------------------------
    # Server Runner
    # -----------------------------------------------------------------------
    async def run(self):
        """
        Launch the MCP server using STDIO transport.
        Handles initialization, logging, and stream lifecycle.
        """
        self.initialize_clients()

        # Server banner
        logger.info("=" * 60)
        logger.info("Databricks MCP Server (Modular Edition)")
        logger.info("=" * 60)
        logger.info("Handlers initialized:")
        logger.info("  - ResourceHandler: Unity Catalog resources")
        logger.info("  - PromptHandler: Prompt templates")
        logger.info("  - ToolHandler: Tool implementations")
        logger.info("=" * 60)

        # Run the MCP stdio server
        async with stdio_server() as (read_stream, write_stream):
            await self.app.run(
                read_stream,
                write_stream,
                self.app.create_initialization_options(),
            )


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------
def main():
    """
    Application entry point.
    Loads environment variables, creates the MCP server, and starts execution.
    """
    from dotenv import load_dotenv

    # Load .env file
    load_dotenv()

    # Create and execute the server instance
    server = DatabricksMCPServer()
    asyncio.run(server.run())


# ---------------------------------------------------------------------------
# Script Invocation
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    main()

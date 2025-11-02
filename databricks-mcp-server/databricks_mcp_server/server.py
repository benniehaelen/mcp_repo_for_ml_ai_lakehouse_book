"""
Databricks Unity Catalog MCP Server (Modular Version)
Provides tools, resources, and prompts for querying Databricks Unity Catalog

This is a refactored version with separated implementation modules for:
- Resources (resources.py)
- Prompts (prompts.py)  
- Tools (tools.py)
"""
import asyncio
import logging
import os
from typing import Any, Optional, Sequence

from anthropic import Anthropic
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from pydantic import ValidationError

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

from databricks_mcp_server.implementation import (
    ResourceHandler,
    PromptHandler,
    ToolHandler,
)
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
    # Error handling
    ErrorOutput,
)

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabricksMCPServer:
    """
    Modular MCP Server for Databricks Unity Catalog
    
    Architecture:
    - ResourceHandler: Manages Unity Catalog resources
    - PromptHandler: Manages prompt templates
    - ToolHandler: Implements all tool functionality
    """
    
    def __init__(self):
        self.app = Server("databricks-mcp-server")
        
        # Databricks and AI clients
        self.workspace_client: Optional[WorkspaceClient] = None
        self.anthropic_client: Optional[Anthropic] = None
        
        # Implementation handlers
        self.resource_handler = ResourceHandler()
        self.prompt_handler = PromptHandler()
        self.tool_handler = ToolHandler()
        
        # Setup MCP protocol handlers
        self._setup_handlers()
    
    def _setup_handlers(self):
        """Setup all MCP protocol handlers using implementation modules"""
        
        # =====================================================================
        # Resource Handlers
        # =====================================================================
        
        @self.app.list_resources()
        async def list_resources() -> list[Resource]:
            """List available Unity Catalog resources"""
            return await self.resource_handler.list_resources()
        
        @self.app.read_resource()
        async def read_resource(uri: str) -> str:
            """Read a Unity Catalog resource"""
            content_text = await self.resource_handler.read_resource(uri)
            
            # Return in the correct MCP format
            return content_text  # MCP SDK will wrap this automatically
        
        @self.app.list_resource_templates()
        async def list_resource_templates() -> list[ResourceTemplate]:
            """List available resource templates"""
            from mcp.types import ResourceTemplate
            
            return [
                ResourceTemplate(
                    uriTemplate="databricks://catalog/{catalog_name}",
                    name="Catalog Schemas",
                    description="Get schemas for any catalog by name",
                    mimeType="application/json"
                ),
                ResourceTemplate(
                    uriTemplate="databricks://table/{catalog_name}/{schema_name}/{table_name}",
                    name="Table Information",
                    description="Get detailed information about any table",
                    mimeType="application/json"
                )
            ]        
        
        # =====================================================================
        # Prompt Handlers
        # =====================================================================
        
        @self.app.list_prompts()
        async def list_prompts() -> list[Prompt]:
            """List available prompt templates"""
            return await self.prompt_handler.list_prompts()
        
        @self.app.get_prompt()
        async def get_prompt(name: str, arguments: dict[str, str] | None) -> GetPromptResult:
            """Get a specific prompt template"""
            return await self.prompt_handler.get_prompt(name, arguments)
        
        # =====================================================================
        # Tool Handlers
        # =====================================================================
        
        @self.app.list_tools()
        async def list_tools() -> list[Tool]:
            """List available tools"""
            return self.tool_handler.get_tool_definitions()
        
        @self.app.call_tool()
        async def call_tool(
            name: str,
            arguments: Any
        ) -> Sequence[TextContent | ImageContent | EmbeddedResource]:
            """Execute a tool with Pydantic validation"""
            try:
                # Route to appropriate tool handler
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
                
                else:
                    error = ErrorOutput(error=f"Unknown tool: {name}")
                    return [TextContent(type="text", text=format_tool_output(error))]
                    
            except ValidationError as e:
                error = ErrorOutput(
                    error="Validation error",
                    details=str(e)
                )
                return [TextContent(type="text", text=format_tool_output(error))]
            
            except Exception as e:
                logger.error(f"Error executing tool {name}: {e}", exc_info=True)
                error = ErrorOutput(error=str(e))
                return [TextContent(type="text", text=format_tool_output(error))]
    
    def initialize_clients(self):
        """Initialize Databricks and Anthropic clients"""
        try:
            # Initialize Databricks client
            config = Config(
                host=os.getenv("DATABRICKS_HOST"),
                token=os.getenv("DATABRICKS_TOKEN")
            )
            self.workspace_client = WorkspaceClient(config=config)
            logger.info("✓ Databricks client initialized")
            
            # Set workspace client in handlers
            self.resource_handler.set_workspace_client(self.workspace_client)
            self.tool_handler.set_workspace_client(self.workspace_client)
            
            # Initialize Anthropic client if API key is available
            api_key = os.getenv("ANTHROPIC_API_KEY")
            if api_key:
                self.anthropic_client = Anthropic(api_key=api_key)
                self.tool_handler.set_anthropic_client(self.anthropic_client)
                logger.info("✓ Anthropic client initialized")
            else:
                logger.warning("⚠ ANTHROPIC_API_KEY not set, NL queries will not work")
                
        except Exception as e:
            logger.error(f"✗ Error initializing clients: {e}", exc_info=True)
            raise
    
    async def run(self):
        """Run the MCP server"""
        self.initialize_clients()
        
        logger.info("=" * 60)
        logger.info("Databricks MCP Server (Modular)")
        logger.info("=" * 60)
        logger.info("Handlers initialized:")
        logger.info("  - ResourceHandler: Unity Catalog resources")
        logger.info("  - PromptHandler: Prompt templates")
        logger.info("  - ToolHandler: Tool implementations")
        logger.info("=" * 60)
        
        async with stdio_server() as (read_stream, write_stream):
            await self.app.run(
                read_stream,
                write_stream,
                self.app.create_initialization_options()
            )


def main():
    """Main entry point"""
    from dotenv import load_dotenv
    
    # Load environment variables
    load_dotenv()
    
    # Create and run server
    server = DatabricksMCPServer()
    asyncio.run(server.run())


if __name__ == "__main__":
    main()
"""
Prompt handlers for Databricks MCP Server
Handles prompt template listing and generation
"""
from typing import List, Optional, Dict
from mcp.types import Prompt, PromptMessage, TextContent, GetPromptResult


class PromptHandler:
    """Handles all MCP prompt operations"""
    
    def __init__(self):
        self._prompts = self._initialize_prompts()
    
    def _initialize_prompts(self) -> List[Prompt]:
        """Initialize available prompt templates"""
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
    
    async def list_prompts(self) -> List[Prompt]:
        """List available prompt templates"""
        return self._prompts
    
    async def get_prompt(self, name: str, arguments: Optional[Dict[str, str]] = None) -> GetPromptResult:
        """Get a specific prompt template with filled arguments"""
        args = arguments or {}
        
        if name == "query-table":
            return self._get_query_table_prompt(args)
        elif name == "analyze-data":
            return self._get_analyze_data_prompt(args)
        elif name == "explore-catalog":
            return self._get_explore_catalog_prompt(args)
        else:
            raise ValueError(f"Unknown prompt: {name}")
    
    def _get_query_table_prompt(self, args: Dict[str, str]) -> GetPromptResult:
        """Generate SQL query prompt for a table"""
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
    
    def _get_analyze_data_prompt(self, args: Dict[str, str]) -> GetPromptResult:
        """Generate data analysis prompt"""
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
    
    def _get_explore_catalog_prompt(self, args: Dict[str, str]) -> GetPromptResult:
        """Generate catalog exploration prompt"""
        catalog = args.get("catalog", "")
        prompt_text = "Explore the Unity Catalog structure"
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
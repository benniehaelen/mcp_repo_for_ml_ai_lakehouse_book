"""
Pydantic models for Databricks MCP Server tool inputs and outputs
"""
from typing import Optional, Literal, List, Dict, Any
from pydantic import BaseModel, Field, field_validator
from enum import Enum


# ============================================================================
# Tool Input Models
# ============================================================================

class ListCatalogsInput(BaseModel):
    """Input for list_catalogs tool - no parameters needed"""
    pass


class ListSchemasInput(BaseModel):
    """Input for list_schemas tool"""
    catalog: str = Field(
        ...,
        description="Name of the catalog to list schemas from",
        min_length=1
    )


class ListTablesInput(BaseModel):
    """Input for list_tables tool"""
    catalog: str = Field(
        ...,
        description="Name of the catalog",
        min_length=1
    )
    schema_name: str = Field(
        ...,
        description="Name of the schema",
        min_length=1,
        alias="schema_name"  # Allow both 'schema' and 'schema_name'
    )


class GetTableInfoInput(BaseModel):
    """Input for get_table_info tool"""
    catalog: str = Field(
        ...,
        description="Name of the catalog",
        min_length=1
    )
    schema_name: str = Field(
        ...,
        description="Name of the schema",
        min_length=1
    )
    table: str = Field(
        ...,
        description="Name of the table",
        min_length=1
    )


class ExecuteSQLInput(BaseModel):
    """Input for execute_sql tool"""
    query: str = Field(
        ...,
        description="SQL query to execute",
        min_length=1
    )
    warehouse_id: Optional[str] = Field(
        None,
        description="SQL warehouse ID (uses environment default if not provided)"
    )
    
    @field_validator('query')
    @classmethod
    def validate_query(cls, v: str) -> str:
        """Validate that query is not empty or just whitespace"""
        if not v or not v.strip():
            raise ValueError("Query cannot be empty")
        return v.strip()


class QueryNaturalLanguageInput(BaseModel):
    """Input for query_natural_language tool"""
    question: str = Field(
        ...,
        description="Natural language question to convert to SQL",
        min_length=1
    )
    catalog: str = Field(
        ...,
        description="Name of the catalog",
        min_length=1
    )
    schema_name: str = Field(
        ...,
        description="Name of the schema",
        min_length=1
    )
    table: str = Field(
        ...,
        description="Name of the table",
        min_length=1
    )
    warehouse_id: Optional[str] = Field(
        None,
        description="SQL warehouse ID (uses environment default if not provided)"
    )


class ChartType(str, Enum):
    """Supported chart types"""
    BAR = "bar"
    LINE = "line"
    SCATTER = "scatter"
    PIE = "pie"
    HISTOGRAM = "histogram"
    BOX = "box"


class CreateChartInput(BaseModel):
    """Input for create_chart tool"""
    query: str = Field(
        ...,
        description="SQL query to get data for the chart",
        min_length=1
    )
    chart_type: ChartType = Field(
        ...,
        description="Type of chart to create"
    )
    x_column: Optional[str] = Field(
        None,
        description="Column name for X axis (auto-detected if not provided)"
    )
    y_column: Optional[str] = Field(
        None,
        description="Column name for Y axis (auto-detected if not provided)"
    )
    title: str = Field(
        default="Chart",
        description="Title for the chart"
    )
    warehouse_id: Optional[str] = Field(
        None,
        description="SQL warehouse ID (uses environment default if not provided)"
    )


# ============================================================================
# Tool Output Models
# ============================================================================

class CatalogInfo(BaseModel):
    """Information about a catalog"""
    name: str
    comment: Optional[str] = None
    owner: Optional[str] = None
    created_at: Optional[str] = None


class ListCatalogsOutput(BaseModel):
    """Output for list_catalogs tool"""
    catalogs: List[CatalogInfo]


class SchemaInfo(BaseModel):
    """Information about a schema"""
    name: str
    comment: Optional[str] = None
    owner: Optional[str] = None
    created_at: Optional[str] = None


class ListSchemasOutput(BaseModel):
    """Output for list_schemas tool"""
    catalog: str
    schemas: List[SchemaInfo]


class TableInfo(BaseModel):
    """Basic information about a table"""
    name: str
    table_type: Optional[str] = None
    comment: Optional[str] = None
    owner: Optional[str] = None


class ListTablesOutput(BaseModel):
    """Output for list_tables tool"""
    catalog: str
    schema_name: str
    tables: List[TableInfo]


class ColumnInfo(BaseModel):
    """Information about a table column"""
    name: str
    type_name: Optional[str] = None
    type_text: Optional[str] = None
    comment: Optional[str] = None
    nullable: Optional[bool] = None
    position: Optional[int] = None


class DetailedTableInfo(BaseModel):
    """Detailed information about a table"""
    name: str
    catalog_name: str
    schema_name: str
    table_type: Optional[str] = None
    data_source_format: Optional[str] = None
    columns: List[ColumnInfo]
    owner: Optional[str] = None
    comment: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None


class ExecuteSQLOutput(BaseModel):
    """Output for execute_sql tool"""
    status: str
    row_count: Optional[int] = None
    columns: Optional[List[str]] = None
    data: Optional[List[Dict[str, Any]]] = None
    message: Optional[str] = None


class QueryNaturalLanguageOutput(BaseModel):
    """Output for query_natural_language tool"""
    generated_sql: str
    execution_result: ExecuteSQLOutput


class CreateChartOutput(BaseModel):
    """Output for create_chart tool"""
    status: str
    message: str
    chart_type: str
    image_data: Optional[str] = Field(
        None,
        description="Base64 encoded PNG image data"
    )
    mime_type: Optional[str] = Field(
        None,
        description="MIME type of the image (typically image/png)"
    )


class ErrorOutput(BaseModel):
    """Generic error output"""
    error: str
    details: Optional[str] = None


# ============================================================================
# Helper functions to convert Pydantic models to JSON Schema
# ============================================================================

def get_tool_input_schema(model: type[BaseModel]) -> dict:
    """Convert a Pydantic model to MCP tool input schema"""
    schema = model.model_json_schema()
    
    # MCP expects inputSchema format
    return {
        "type": "object",
        "properties": schema.get("properties", {}),
        "required": schema.get("required", []),
        "additionalProperties": False
    }


def parse_tool_input(model: type[BaseModel], arguments: dict) -> BaseModel:
    """Parse and validate tool input using Pydantic model"""
    return model.model_validate(arguments)


def format_tool_output(output: BaseModel) -> str:
    """Format tool output as JSON string"""
    return output.model_dump_json(indent=2, exclude_none=False)
"""
Resource handlers for Databricks MCP Server
Handles Unity Catalog resource listing and reading
"""
import logging
from typing import Optional, List
from databricks.sdk import WorkspaceClient
from mcp.types import Resource

from databricks_mcp_server.pydantic_models import (
    ListCatalogsOutput,
    CatalogInfo,
    ListSchemasOutput,
    SchemaInfo,
    ErrorOutput,
    format_tool_output,
)

logger = logging.getLogger(__name__)


class ResourceHandler:
    """Handles all MCP resource operations"""
    
    def __init__(self, workspace_client: Optional[WorkspaceClient] = None):
        self.workspace_client = workspace_client
    
    def set_workspace_client(self, client: WorkspaceClient):
        """Set the Databricks workspace client"""
        self.workspace_client = client
    
    async def list_resources(self) -> List[Resource]:
        """List available Unity Catalog resources"""
        resources = []
        
        if not self.workspace_client:
            return resources
        
        try:
            # List catalogs resource
            resources.append(Resource(
                uri="databricks://catalogs",
                name="Unity Catalog - Catalogs",
                mimeType="application/json",
                description="List of all catalogs in Unity Catalog"
            ))
            
            # Add catalog-specific resources
            catalogs = list(self.workspace_client.catalogs.list())
            for catalog in catalogs:
                resources.append(Resource(
                    uri=f"databricks://catalog/{catalog.name}",
                    name=f"Catalog: {catalog.name}",
                    mimeType="application/json",
                    description=f"Schemas in catalog {catalog.name}"
                ))
                
        except Exception as e:
            logger.error(f"Error listing resources: {e}")
        
        return resources
    
    async def read_resource(self, uri: str) -> str:
        """Read a Unity Catalog resource by URI"""
        if not self.workspace_client:
            error = ErrorOutput(error="Databricks client not initialized")
            return format_tool_output(error)
        
        # Convert uri to string (in case it's a Pydantic URL object)
        uri_str = str(uri)
        
        try:
            # Handle catalogs resource
            if uri_str == "databricks://catalogs":
                return await self._read_catalogs()
            
            # Handle catalog-specific resource
            elif uri_str.startswith("databricks://catalog/"):
                catalog_name = uri_str.replace("databricks://catalog/", "")
                return await self._read_catalog_schemas(catalog_name)
            
            # Handle table-specific resource
            elif uri_str.startswith("databricks://table/"):
                parts = uri_str.replace("databricks://table/", "").split("/")
                if len(parts) == 3:
                    catalog, schema, table = parts
                    return await self._read_table_info(catalog, schema, table)
            
            # Unknown resource URI
            error = ErrorOutput(error=f"Invalid resource URI: {uri_str}")
            return format_tool_output(error)
            
        except Exception as e:
            logger.error(f"Error reading resource {uri_str}: {e}", exc_info=True)
            error = ErrorOutput(error=str(e), details=str(e.__class__.__name__))
            return format_tool_output(error)



    async def _read_catalogs(self) -> str:
        """Read all catalogs"""
        catalogs = list(self.workspace_client.catalogs.list())
        output = ListCatalogsOutput(
            catalogs=[
                CatalogInfo(
                    name=c.name,
                    comment=c.comment,
                    owner=c.owner,
                    created_at=str(c.created_at) if c.created_at else None
                )
                for c in catalogs
            ]
        )
        return format_tool_output(output)
    
    async def _read_catalog_schemas(self, catalog_name: str) -> str:
        """Read schemas in a catalog"""
        schemas = list(self.workspace_client.schemas.list(catalog_name=catalog_name))
        output = ListSchemasOutput(
            catalog=catalog_name,
            schemas=[
                SchemaInfo(
                    name=s.name,
                    comment=s.comment,
                    owner=s.owner,
                    created_at=str(s.created_at) if s.created_at else None
                )
                for s in schemas
            ]
        )
        return format_tool_output(output)
    
    async def _read_table_info(self, catalog: str, schema: str, table: str) -> str:
        """Read detailed table information"""
        table_info = self.workspace_client.tables.get(
            full_name=f"{catalog}.{schema}.{table}"
        )
        
        # Create a simplified table info output
        from databricks_mcp_server.models import DetailedTableInfo, ColumnInfo
        
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
        return format_tool_output(output)
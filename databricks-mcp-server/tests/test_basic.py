"""
Basic tests for Databricks MCP Server
Run with: pytest tests/
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from databricks_mcp_server.server import DatabricksMCPServer


class TestDatabricksMCPServer:
    """Test the MCP server functionality"""
    
    def test_server_initialization(self):
        """Test that the server initializes correctly"""
        server = DatabricksMCPServer()
        assert server.app is not None
        assert server.workspace_client is None
        assert server.anthropic_client is None
    
    @pytest.mark.asyncio
    async def test_list_catalogs_tool(self):
        """Test list_catalogs tool execution"""
        server = DatabricksMCPServer()
        
        # Mock the workspace client
        mock_client = Mock()
        mock_catalog = Mock()
        mock_catalog.name = "test_catalog"
        mock_catalog.comment = "Test catalog"
        mock_catalog.owner = "admin"
        
        mock_client.catalogs.list.return_value = [mock_catalog]
        server.workspace_client = mock_client
        
        # Execute the tool
        result = await server._list_catalogs()
        
        # Verify result
        assert len(result) == 1
        assert "test_catalog" in result[0].text
    
    @pytest.mark.asyncio
    async def test_list_schemas_tool(self):
        """Test list_schemas tool execution"""
        server = DatabricksMCPServer()
        
        # Mock the workspace client
        mock_client = Mock()
        mock_schema = Mock()
        mock_schema.name = "test_schema"
        mock_schema.comment = "Test schema"
        mock_schema.owner = "admin"
        
        mock_client.schemas.list.return_value = [mock_schema]
        server.workspace_client = mock_client
        
        # Execute the tool
        result = await server._list_schemas("test_catalog")
        
        # Verify result
        assert len(result) == 1
        assert "test_schema" in result[0].text
    
    @pytest.mark.asyncio
    async def test_list_tables_tool(self):
        """Test list_tables tool execution"""
        server = DatabricksMCPServer()
        
        # Mock the workspace client
        mock_client = Mock()
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.table_type = Mock()
        mock_table.table_type.value = "MANAGED"
        mock_table.comment = "Test table"
        mock_table.owner = "admin"
        
        mock_client.tables.list.return_value = [mock_table]
        server.workspace_client = mock_client
        
        # Execute the tool
        result = await server._list_tables("test_catalog", "test_schema")
        
        # Verify result
        assert len(result) == 1
        assert "test_table" in result[0].text
    
    @pytest.mark.asyncio
    async def test_get_table_info_tool(self):
        """Test get_table_info tool execution"""
        server = DatabricksMCPServer()
        
        # Mock the workspace client
        mock_client = Mock()
        mock_table = Mock()
        mock_table.name = "test_table"
        mock_table.catalog_name = "test_catalog"
        mock_table.schema_name = "test_schema"
        mock_table.table_type = Mock()
        mock_table.table_type.value = "MANAGED"
        mock_table.data_source_format = None
        mock_table.owner = "admin"
        mock_table.comment = "Test table"
        mock_table.properties = {}
        
        # Mock columns
        mock_col = Mock()
        mock_col.name = "id"
        mock_col.type_name = Mock()
        mock_col.type_name.value = "INT"
        mock_col.type_text = "int"
        mock_col.comment = "ID column"
        mock_col.nullable = False
        mock_col.position = 0
        mock_table.columns = [mock_col]
        
        mock_client.tables.get.return_value = mock_table
        server.workspace_client = mock_client
        
        # Execute the tool
        result = await server._get_table_info("test_catalog", "test_schema", "test_table")
        
        # Verify result
        assert len(result) == 1
        assert "test_table" in result[0].text
        assert "id" in result[0].text


class TestDatabricksMCPClient:
    """Test the MCP client functionality"""
    
    @pytest.mark.asyncio
    async def test_client_connection(self):
        """Test that client can connect (mocked)"""
        from databricks_mcp_server.client import DatabricksMCPClient
        
        client = DatabricksMCPClient()
        assert client.server_script_path == "databricks-mcp-server"
        assert client.session is None


def test_package_imports():
    """Test that all package modules can be imported"""
    import databricks_mcp_server
    from databricks_mcp_server import server
    from databricks_mcp_server import client
    
    assert databricks_mcp_server.__version__ == "0.1.0"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

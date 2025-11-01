# Architecture Documentation

## Overview

The Databricks MCP Server is a Model Context Protocol (MCP) implementation that bridges Databricks Unity Catalog with MCP-compatible clients. It provides a standardized interface for exploring metadata, executing queries, and visualizing data.

## System Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                         MCP Client Layer                             │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────────┐ │
│  │ Claude       │  │ Python       │  │ Databricks Notebook      │ │
│  │ Desktop      │  │ Async Client │  │ Client                   │ │
│  └──────────────┘  └──────────────┘  └──────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │ MCP Protocol (stdio/SSE)
                              │
┌─────────────────────────────────────────────────────────────────────┐
│                       MCP Server Layer                               │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ DatabricksMCPServer                                            │ │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────────┐   │ │
│  │  │ Tools       │  │ Resources   │  │ Prompts             │   │ │
│  │  │ Handler     │  │ Handler     │  │ Handler             │   │ │
│  │  └─────────────┘  └─────────────┘  └─────────────────────┘   │ │
│  └────────────────────────────────────────────────────────────────┘ │
│  ┌────────────────────────────────────────────────────────────────┐ │
│  │ Service Layer                                                  │ │
│  │  ┌──────────────────┐  ┌──────────────────┐                  │ │
│  │  │ Query Executor   │  │ Chart Generator  │                  │ │
│  │  └──────────────────┘  └──────────────────┘                  │ │
│  │  ┌──────────────────┐  ┌──────────────────┐                  │ │
│  │  │ NL to SQL Conv.  │  │ Metadata Manager │                  │ │
│  │  └──────────────────┘  └──────────────────┘                  │ │
│  └────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │ SDK Calls
                              │
┌─────────────────────────────────────────────────────────────────────┐
│                     External Services                                │
│  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐ │
│  │ Databricks       │  │ Anthropic        │  │ Plotly           │ │
│  │ Unity Catalog    │  │ Claude API       │  │ Visualization    │ │
│  └──────────────────┘  └──────────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────────────┘
```

## Component Details

### 1. MCP Server (databricks_mcp_server/server.py)

**Purpose**: Core MCP protocol implementation

**Key Components**:
- `DatabricksMCPServer`: Main server class
- Protocol handlers for tools, resources, and prompts
- Integration with Databricks SDK and Anthropic API

**Responsibilities**:
- Handle MCP protocol messages via stdio
- Route tool calls to appropriate handlers
- Manage resources (Unity Catalog metadata)
- Provide prompt templates

### 2. Python Client (databricks_mcp_server/client.py)

**Purpose**: Asynchronous Python client for MCP server

**Key Features**:
- Context manager for connection lifecycle
- High-level async API for all server operations
- Result parsing and formatting

**Usage Pattern**:
```python
async with client.connect():
    result = await client.execute_sql(query)
```

### 3. Notebook Client (databricks_mcp_notebook_client.py)

**Purpose**: Direct Databricks SDK integration for notebooks

**Key Differences from MCP Client**:
- No separate server process required
- Synchronous API (suitable for notebooks)
- Direct access to Databricks SDK
- Integrated with Databricks dbutils

**Usage Pattern**:
```python
client = NotebookMCPClient()
df = client.execute_sql(query)
display(df)
```

## Protocol Implementation

### MCP Tools

Tools are the primary way clients interact with the server. Each tool has:
- Name (unique identifier)
- Description (human-readable purpose)
- Input schema (JSON Schema for parameters)
- Handler function (async implementation)

**Implemented Tools**:

1. **list_catalogs**: List Unity Catalogs
   - Parameters: None
   - Returns: JSON array of catalogs

2. **list_schemas**: List schemas in a catalog
   - Parameters: catalog (string)
   - Returns: JSON array of schemas

3. **list_tables**: List tables in a schema
   - Parameters: catalog, schema (strings)
   - Returns: JSON array of tables

4. **get_table_info**: Get table metadata
   - Parameters: catalog, schema, table (strings)
   - Returns: JSON object with columns, types, comments

5. **execute_sql**: Execute SQL query
   - Parameters: query (string), warehouse_id (optional string)
   - Returns: JSON with status and data

6. **query_natural_language**: NL to SQL conversion and execution
   - Parameters: question, catalog, schema, table, warehouse_id
   - Returns: Generated SQL and query results

7. **create_chart**: Generate Plotly charts
   - Parameters: query, chart_type, x_column, y_column, title, warehouse_id
   - Returns: Chart metadata and base64 image

### MCP Resources

Resources represent data that can be read:

**Resource URIs**:
- `databricks://catalogs` - All catalogs
- `databricks://catalog/{name}` - Specific catalog schemas
- `databricks://table/{catalog}/{schema}/{table}` - Table metadata

**Resource Format**: JSON with hierarchical structure

### MCP Prompts

Prompts are templates for common tasks:

1. **query-table**: Generate SQL for a table
2. **analyze-data**: Analyze query results
3. **explore-catalog**: Explore catalog structure

## Data Flow

### 1. SQL Query Execution

```
Client Request
    ↓
MCP Server (execute_sql tool)
    ↓
Databricks SDK (statement_execution API)
    ↓
SQL Warehouse (query execution)
    ↓
Result Processing (to DataFrame)
    ↓
JSON Response (with data)
    ↓
Client Receives Results
```

### 2. Natural Language Query

```
Client Request (NL question)
    ↓
MCP Server (query_natural_language tool)
    ↓
Get Table Schema (from Unity Catalog)
    ↓
Anthropic API (Claude converts NL to SQL)
    ↓
SQL Query Execution (via execute_sql)
    ↓
Results + Generated SQL
    ↓
Client Receives Both
```

### 3. Chart Generation

```
Client Request (query + chart type)
    ↓
MCP Server (create_chart tool)
    ↓
Execute SQL Query
    ↓
Convert to Pandas DataFrame
    ↓
Plotly Chart Generation
    ↓
Chart to PNG Image
    ↓
Base64 Encode
    ↓
Return Image + Metadata
    ↓
Client Saves/Displays Chart
```

## Technology Stack

### Core Dependencies
- **mcp (>=0.9.0)**: Model Context Protocol SDK
- **databricks-sdk (>=0.20.0)**: Databricks workspace and Unity Catalog access
- **plotly (>=5.18.0)**: Chart generation
- **pandas (>=2.0.0)**: Data manipulation
- **anthropic (>=0.21.0)**: Natural language processing
- **python-dotenv (>=1.0.0)**: Environment configuration

### Development Dependencies
- **pytest**: Testing framework
- **pytest-asyncio**: Async test support

## Security Considerations

### Authentication
- Databricks: Personal Access Token (PAT)
- Anthropic: API Key
- Both stored in environment variables

### Data Access
- Respects Unity Catalog permissions
- SQL warehouse security applies
- No data caching (queries run on demand)

### Best Practices
1. Use short-lived tokens when possible
2. Store credentials in secure secret management systems
3. Use Databricks Secret Scopes in production
4. Implement rate limiting for API calls
5. Audit all query executions

## Performance Considerations

### Query Execution
- Async operations for non-blocking I/O
- Configurable timeouts (default 30s)
- Result pagination for large datasets

### Chart Generation
- Image generation can be memory-intensive
- Limit data points for performance (max 10,000 recommended)
- Use appropriate chart types for data size

### Connection Management
- Single workspace client per server instance
- Connection pooling handled by Databricks SDK
- Graceful error handling and retries

## Extension Points

### Adding New Tools
1. Define tool schema in `list_tools()`
2. Implement handler function
3. Register in `call_tool()` dispatcher
4. Add tests

### Custom Chart Types
1. Extend `_create_chart()` method
2. Add chart type to schema enum
3. Implement Plotly logic
4. Update documentation

### Additional Resources
1. Define resource URI pattern
2. Implement in `read_resource()`
3. Add to `list_resources()`
4. Document resource structure

## Monitoring and Debugging

### Logging
- Standard Python logging framework
- Configurable log levels (DEBUG, INFO, WARNING, ERROR)
- Structured logging for production

### Error Handling
- Graceful degradation
- Detailed error messages
- Exception tracking and reporting

### Metrics (Future Enhancement)
- Query execution times
- Tool call frequency
- Error rates
- Resource usage

## Future Enhancements

### Planned Features
1. **Streaming Support**: Progressive results for long queries
2. **Caching**: Query result caching
3. **Batch Operations**: Multiple queries in one call
4. **Advanced Analytics**: Statistical analysis tools
5. **Export Formats**: CSV, Excel, Parquet export
6. **Scheduling**: Automated query scheduling
7. **Alerts**: Query result monitoring and alerts

### Integration Opportunities
1. MLflow integration for model tracking
2. Delta Live Tables support
3. Databricks Jobs orchestration
4. dbt integration
5. BI tool connectors

## Testing Strategy

### Unit Tests
- Mock Databricks SDK responses
- Test tool handlers independently
- Validate JSON schemas

### Integration Tests
- Real Databricks workspace (test environment)
- End-to-end query execution
- Chart generation validation

### Performance Tests
- Load testing with concurrent requests
- Large dataset handling
- Memory profiling

## Deployment Options

### 1. Local Development
- Run server locally via stdio
- Connect with Python client
- Development and testing

### 2. Databricks Notebooks
- Import notebook client directly
- No separate server needed
- Interactive data exploration

### 3. Production Server
- Deploy as long-running service
- Multiple clients connect via MCP
- Load balancing and high availability

### 4. Claude Desktop Integration
- Configure as MCP server
- Use from Claude Desktop app
- Natural language interface

## Conclusion

The Databricks MCP Server provides a robust, extensible foundation for integrating Unity Catalog with the Model Context Protocol ecosystem. Its modular architecture enables easy customization while maintaining compatibility with the MCP standard.

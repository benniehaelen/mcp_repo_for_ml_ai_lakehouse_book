# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Databricks MCP Server — a Model Context Protocol server for Databricks Unity Catalog that enables metadata exploration, SQL/NL query execution, and data visualization. All code lives under `databricks-mcp-server/`.

## Common Commands

```bash
# Install (from databricks-mcp-server/ directory)
uv pip install -e .          # preferred
pip install -e .             # alternative

# Run the server
databricks-mcp-server        # entry point after install
python -m databricks_mcp_server.server

# Tests
pytest tests/ -v
pytest tests/test_basic.py   # single test file

# Build
uv build
```

There is no configured linter or formatter in the project.

## Architecture

Three-layer design using the MCP protocol over stdio:

```
Clients (Claude Desktop / Python async client / Notebook client)
  → DatabricksMCPServer (server.py) — dispatches via MCP protocol
    → Handler Layer:
        ResourceHandler (resources.py)  — Unity Catalog metadata
        PromptHandler (prompts.py)      — prompt templates
        ToolHandler (tools.py)          — SQL execution, NL→SQL, charts
      → External: Databricks SDK, Anthropic API, Plotly
```

**Key files:**
- `server.py` — MCP server core, tool dispatch, connection management
- `client.py` — async Python client with context manager pattern
- `pydantic_models.py` — all input/output models and schema conversion helpers (`get_tool_input_schema()`, `parse_tool_input()`, `format_tool_output()`)
- `implementation/tools.py` — 7 tools: `list_catalogs`, `list_schemas`, `list_tables`, `get_table_info`, `execute_sql`, `query_natural_language`, `create_chart`
- `implementation/resources.py` — 3 resource URI patterns (`databricks://catalogs`, `databricks://catalog/{name}`, `databricks://table/{catalog}/{schema}/{table}`)
- `implementation/prompts.py` — 3 prompt templates (query-table, analyze-data, explore-catalog)
- `databricks_mcp_notebook_client.py` — synchronous notebook client returning Pandas DataFrames

## Key Patterns

- **Async/await throughout** — all I/O is non-blocking
- **Pydantic validation** — every tool has typed input/output models in `pydantic_models.py`
- **Context manager** — client connections use `async with`
- **Environment config** — secrets via `.env` file (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`, optional `ANTHROPIC_API_KEY`)

## Dependencies

Python >=3.11. Core: `mcp>=0.9.0`, `databricks-sdk>=0.20.0`, `plotly>=5.18.0`, `pandas>=2.0.0`, `anthropic>=0.21.0`, `python-dotenv`, `httpx`. Dev: `pytest`, `pytest-asyncio`. Build backend: hatchling.

## Data Flow

- **SQL execution:** client → `execute_sql` tool → Pydantic validation → Databricks SDK `statement_execution` → JSON response
- **NL query:** client question → fetch table schema from Unity Catalog → Anthropic Claude generates SQL → execute SQL → return both SQL and results
- **Chart generation:** SQL query → execute → Pandas DataFrame → Plotly chart → base64 PNG

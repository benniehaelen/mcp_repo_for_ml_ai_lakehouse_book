#!/usr/bin/env bash
# Setup script for Databricks MCP Server

set -e

echo "=================================================="
echo "Databricks MCP Server - Setup Script"
echo "=================================================="
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: Python 3 is not installed"
    echo "Please install Python 3.11 or higher"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "✓ Found Python $PYTHON_VERSION"

# Check if UV is available
if command -v uv &> /dev/null; then
    echo "✓ Found UV package manager"
    USE_UV=true
else
    echo "⚠ UV not found, will use pip"
    USE_UV=false
fi

echo ""
echo "Step 1: Creating environment file..."
if [ ! -f .env ]; then
    cp .env.example .env
    echo "✓ Created .env file from template"
    echo "⚠ Please edit .env with your credentials before running the server"
else
    echo "✓ .env file already exists"
fi

echo ""
echo "Step 2: Installing dependencies..."
if [ "$USE_UV" = true ]; then
    uv pip install -e .
    echo "✓ Installed with UV"
else
    python3 -m pip install -e .
    echo "✓ Installed with pip"
fi

echo ""
echo "Step 3: Installing development dependencies..."
if [ "$USE_UV" = true ]; then
    uv pip install pytest pytest-asyncio --break-system-packages 2>/dev/null || true
else
    python3 -m pip install pytest pytest-asyncio
fi
echo "✓ Development dependencies installed"

echo ""
echo "=================================================="
echo "✓ Setup Complete!"
echo "=================================================="
echo ""
echo "Next steps:"
echo "  1. Edit .env with your Databricks credentials"
echo "  2. Test connection: python examples/basic_usage.py"
echo "  3. Read QUICKSTART.md for detailed instructions"
echo ""
echo "Run the server:"
echo "  databricks-mcp-server"
echo ""
echo "Run examples:"
echo "  python examples/basic_usage.py"
echo "  python examples/natural_language_queries.py"
echo "  python examples/chart_examples.py"
echo ""
echo "Run tests:"
echo "  pytest tests/"
echo ""

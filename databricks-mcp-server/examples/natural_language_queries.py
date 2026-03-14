"""
Natural language query examples for Databricks MCP Client
Requires ANTHROPIC_API_KEY to be set
"""
import asyncio
import json
from dotenv import load_dotenv
from databricks_mcp_server.client import DatabricksMCPClient

load_dotenv()


async def main():
    """Run natural language query examples"""
    client = DatabricksMCPClient()

    async with client.connect():
        print("=" * 80)
        print("Databricks MCP Client - Natural Language Query Examples")
        print("=" * 80)
        print("\nNote: Make sure ANTHROPIC_API_KEY is set in your environment")
        print()

        # Configuration — uses the NYC Taxi trips sample dataset
        CATALOG = "samples"
        SCHEMA = "nyctaxi"
        TABLE = "trips"

        # Example queries
        questions = [
            "What are the top 10 pickup zip codes by number of trips?",
            "What is the average fare amount and average trip distance?",
            "Show me the top 5 most expensive trips by fare amount",
        ]

        print(f"Target table: {CATALOG}.{SCHEMA}.{TABLE}")
        print("=" * 80)

        for i, question in enumerate(questions, 1):
            print(f"\n{i}. Question: {question}")
            print("-" * 80)

            try:
                result = await client.call_tool("query_natural_language", {
                    "question": question,
                    "catalog": CATALOG,
                    "schema_name": SCHEMA,
                    "table": TABLE,
                })

                print("\nResponse:")
                print(result.get('response', json.dumps(result, indent=2)))

            except Exception as e:
                print(f"Error: {e}")
                print("\nTip: Make sure:")
                print("  1. ANTHROPIC_API_KEY is set")
                print("  2. DATABRICKS_WAREHOUSE_ID is configured")
                print("  3. The table exists and you have access")

        print("\n" + "=" * 80)
        print("Natural language query examples completed!")
        print("=" * 80)


async def custom_query_example():
    """Example: Custom natural language query with user input"""
    client = DatabricksMCPClient()

    async with client.connect():
        # Get table details
        catalog = input("Enter catalog name (default: main): ").strip() or "main"
        schema = input("Enter schema name (default: default): ").strip() or "default"
        table = input("Enter table name: ").strip()

        if not table:
            print("Error: Table name is required")
            return

        # Get table info first
        print(f"\nFetching table info for {catalog}.{schema}.{table}...")
        try:
            table_info = await client.call_tool("get_table_info", {
                "catalog": catalog,
                "schema_name": schema,
                "table": table,
            })
            print("\nTable columns:")
            for col in table_info.get('columns', []):
                print(f"  - {col['name']} ({col.get('type_text', 'unknown')})")
        except Exception as e:
            print(f"Error fetching table info: {e}")
            return

        # Interactive query loop
        print("\n" + "=" * 80)
        print("Interactive Natural Language Query")
        print("=" * 80)
        print("Type your questions (or 'quit' to exit):\n")

        while True:
            question = input("Question: ").strip()

            if question.lower() in ['quit', 'exit', 'q']:
                break

            if not question:
                continue

            try:
                print("\nProcessing...")
                result = await client.call_tool("query_natural_language", {
                    "question": question,
                    "catalog": catalog,
                    "schema_name": schema,
                    "table": table,
                })

                print("\n" + "=" * 80)
                print(result.get('response', json.dumps(result, indent=2)))
                print("=" * 80 + "\n")

            except Exception as e:
                print(f"\n[ERROR] {e}\n")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        asyncio.run(custom_query_example())
    else:
        asyncio.run(main())

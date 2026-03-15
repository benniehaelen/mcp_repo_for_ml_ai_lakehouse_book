"""
MCP Prompt template examples for Databricks MCP Client

Demonstrates how to use MCP prompt templates to generate structured
prompts for LLM interactions. The server exposes three prompt templates:

  query-table      — generate a SQL query for a Unity Catalog table
  analyze-data     — analyze data and surface insights
  explore-catalog  — explore catalog structure
"""
import asyncio
import json
from dotenv import load_dotenv
from databricks_mcp_server.client import DatabricksMCPClient

load_dotenv()


async def main():
    """Run MCP prompt template examples"""
    client = DatabricksMCPClient()

    async with client.connect():
        print("=" * 80)
        print("Databricks MCP Client - Prompt Template Examples")
        print("=" * 80)

        # -----------------------------------------------------------------
        # Example 1: List all available prompt templates
        # -----------------------------------------------------------------
        print("\n1. Listing available prompt templates...")
        print("-" * 80)
        prompts = await client.list_prompts()
        for prompt in prompts:
            print(f"  - {prompt['name']}: {prompt['description']}")
            if prompt.get("arguments"):
                for arg in prompt["arguments"]:
                    name = arg.name if hasattr(arg, "name") else arg["name"]
                    desc = arg.description if hasattr(arg, "description") else arg.get("description", "")
                    required = arg.required if hasattr(arg, "required") else arg.get("required", False)
                    print(f"      {name} ({'required' if required else 'optional'}): {desc}")
            print()

        # -----------------------------------------------------------------
        # Example 2: query-table prompt
        # -----------------------------------------------------------------
        print("\n2. Using 'query-table' prompt template...")
        print("-" * 80)
        result = await client.get_prompt("query-table", {
            "catalog": "samples",
            "schema": "nyctaxi",
            "table": "trips",
            "question": "What are the top 10 pickup zip codes by number of trips?",
        })
        print(f"  Description: {result['description']}")
        print(f"  Generated prompt:")
        for msg in result["messages"]:
            print(f"    [{msg['role']}]")
            print(f"    {msg['content']}")
        print()

        # -----------------------------------------------------------------
        # Example 3: analyze-data prompt
        # -----------------------------------------------------------------
        print("\n3. Using 'analyze-data' prompt template...")
        print("-" * 80)
        result = await client.get_prompt("analyze-data", {
            "data_description": (
                "NYC taxi trip data for January 2024: "
                "21,345 total trips, average fare $18.50, "
                "average distance 3.2 miles. "
                "Peak hours are 8-9 AM and 5-7 PM. "
                "Top pickup zones: Midtown Manhattan, JFK Airport, LaGuardia Airport."
            ),
        })
        print(f"  Description: {result['description']}")
        print(f"  Generated prompt:")
        for msg in result["messages"]:
            print(f"    [{msg['role']}]")
            print(f"    {msg['content']}")
        print()

        # -----------------------------------------------------------------
        # Example 4: explore-catalog prompt (with catalog argument)
        # -----------------------------------------------------------------
        print("\n4. Using 'explore-catalog' prompt template (with catalog)...")
        print("-" * 80)
        result = await client.get_prompt("explore-catalog", {
            "catalog": "samples",
        })
        print(f"  Description: {result['description']}")
        print(f"  Generated prompt:")
        for msg in result["messages"]:
            print(f"    [{msg['role']}]")
            print(f"    {msg['content']}")
        print()

        # -----------------------------------------------------------------
        # Example 5: explore-catalog prompt (without catalog — explores all)
        # -----------------------------------------------------------------
        print("\n5. Using 'explore-catalog' prompt template (no catalog)...")
        print("-" * 80)
        result = await client.get_prompt("explore-catalog", {})
        print(f"  Description: {result['description']}")
        print(f"  Generated prompt:")
        for msg in result["messages"]:
            print(f"    [{msg['role']}]")
            print(f"    {msg['content']}")

        print("\n" + "=" * 80)
        print("Prompt template examples completed!")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

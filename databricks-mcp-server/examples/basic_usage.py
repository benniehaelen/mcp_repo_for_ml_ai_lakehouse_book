"""
Basic usage examples for Databricks MCP Client
"""
import asyncio
import json
from dotenv import load_dotenv
from databricks_mcp_server.client import DatabricksMCPClient

load_dotenv()


async def main():
    """Run basic examples"""
    client = DatabricksMCPClient()

    async with client.connect():
        print("=" * 80)
        print("Databricks MCP Client - Basic Usage Examples")
        print("=" * 80)

        # Show dynamically discovered tools
        print("\n0. Discovered tools from server:")
        print("-" * 80)
        for tool in client.list_tools():
            print(f"  - {tool['name']}: {tool['description']}")

        # Example 1: List catalogs
        print("\n1. Listing all Unity Catalogs...")
        print("-" * 80)
        catalogs = await client.call_tool("list_catalogs")
        print(json.dumps(catalogs, indent=2))

        # Example 2: List resources
        print("\n2. Listing available MCP resources...")
        print("-" * 80)
        resources = await client.list_resources()
        for resource in resources:
            print(f"  - {resource['name']}")
            print(f"     URI: {resource['uri']}")
            print(f"     Type: {resource['mimeType']}")
            print(f"     Description: {resource['description']}")
            print()

        # Example 3: List prompts
        print("\n3. Listing available prompts...")
        print("-" * 80)
        prompts = await client.list_prompts()
        for prompt in prompts:
            print(f"  - {prompt['name']}")
            print(f"     {prompt['description']}")
            if prompt.get('arguments'):
                arg_names = [a.name if hasattr(a, 'name') else a['name'] for a in prompt['arguments']]
                print(f"     Arguments: {', '.join(arg_names)}")

        # Example 4: Explore the "main" catalog (most reliable default)
        catalog_name = "main"
        print(f"\n4. Exploring catalog: {catalog_name}...")
        print("-" * 80)

        schemas = await client.call_tool("list_schemas", {"catalog": catalog_name})
        print(json.dumps(schemas, indent=2))

        # If schemas exist, explore the "default" schema (or the first one)
        if schemas.get('schemas'):
            schema_name = next(
                (s['name'] for s in schemas['schemas'] if s['name'] == 'default'),
                schemas['schemas'][0]['name']
            )
            print(f"\n5. Exploring schema: {catalog_name}.{schema_name}...")
            print("-" * 80)

            tables = await client.call_tool("list_tables", {
                "catalog": catalog_name,
                "schema_name": schema_name,
            })
            print(json.dumps(tables, indent=2))

            # If tables exist, get info on the first one
            if tables.get('tables'):
                table_name = tables['tables'][0]['name']
                print(f"\n6. Getting table info: {catalog_name}.{schema_name}.{table_name}...")
                print("-" * 80)

                table_info = await client.call_tool("get_table_info", {
                    "catalog": catalog_name,
                    "schema_name": schema_name,
                    "table": table_name,
                })
                print(json.dumps(table_info, indent=2))

        # Example 5: Use a prompt template
        print("\n7. Using a prompt template...")
        print("-" * 80)
        prompt_result = await client.get_prompt("explore-catalog", {
            "catalog": "main"
        })
        print(json.dumps(prompt_result, indent=2))

        print("\n" + "=" * 80)
        print("Examples completed!")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

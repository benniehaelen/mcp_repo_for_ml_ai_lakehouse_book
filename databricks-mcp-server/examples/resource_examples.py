"""
MCP Resource examples for Databricks MCP Client

Demonstrates how to use MCP resources to browse Unity Catalog metadata.
Resources provide a read-only, URI-based interface to catalog structure:

  databricks://catalogs                              — all catalogs
  databricks://catalog/{catalog_name}                — schemas in a catalog
  databricks://table/{catalog}/{schema}/{table}      — detailed table info
"""
import asyncio
import json
from dotenv import load_dotenv
from databricks_mcp_server.client import DatabricksMCPClient

load_dotenv()


async def main():
    """Run MCP resource examples"""
    client = DatabricksMCPClient()

    async with client.connect():
        print("=" * 80)
        print("Databricks MCP Client - Resource Examples")
        print("=" * 80)

        # -----------------------------------------------------------------
        # Example 1: List all available resources
        # -----------------------------------------------------------------
        print("\n1. Listing all available MCP resources...")
        print("-" * 80)
        resources = await client.list_resources()
        for resource in resources:
            print(f"  - {resource['name']}")
            print(f"    URI:         {resource['uri']}")
            print(f"    MIME type:   {resource['mimeType']}")
            print(f"    Description: {resource['description']}")
            print()

        # -----------------------------------------------------------------
        # Example 2: Read the catalogs resource
        # -----------------------------------------------------------------
        print("\n2. Reading catalogs resource (databricks://catalogs)...")
        print("-" * 80)
        catalogs_data = await client.read_resource("databricks://catalogs")
        print(json.dumps(catalogs_data, indent=2))

        # -----------------------------------------------------------------
        # Example 3: Read schemas for a specific catalog
        # -----------------------------------------------------------------
        catalog_name = "main"
        print(f"\n3. Reading schemas resource (databricks://catalog/{catalog_name})...")
        print("-" * 80)
        schemas_data = await client.read_resource(f"databricks://catalog/{catalog_name}")
        print(json.dumps(schemas_data, indent=2))

        # -----------------------------------------------------------------
        # Example 4: Read detailed table information via resource URI
        # -----------------------------------------------------------------
        # Use the NYC Taxi sample dataset that ships with Databricks
        catalog = "samples"
        schema = "nyctaxi"
        table = "trips"
        uri = f"databricks://table/{catalog}/{schema}/{table}"
        print(f"\n4. Reading table resource ({uri})...")
        print("-" * 80)
        table_data = await client.read_resource(uri)
        print(json.dumps(table_data, indent=2))

        # -----------------------------------------------------------------
        # Example 5: Browse catalog structure using resources
        # -----------------------------------------------------------------
        print(f"\n5. Browsing catalog structure via resources...")
        print("-" * 80)
        catalogs_data = await client.read_resource("databricks://catalogs")

        for cat in catalogs_data.get("catalogs", []):
            cat_name = cat["name"]
            print(f"\n  Catalog: {cat_name}")
            print(f"    Owner:   {cat.get('owner', 'N/A')}")
            print(f"    Comment: {cat.get('comment', 'N/A')}")

            # Read schemas for this catalog
            try:
                schemas = await client.read_resource(f"databricks://catalog/{cat_name}")
                for s in schemas.get("schemas", []):
                    print(f"      Schema: {s['name']}")
            except Exception as e:
                print(f"      (could not read schemas: {e})")

        print("\n" + "=" * 80)
        print("Resource examples completed!")
        print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())

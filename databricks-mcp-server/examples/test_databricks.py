"""Direct test of Databricks connection without MCP"""
import os
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

# Load environment variables
load_dotenv()

print("="*80)
print("Testing Databricks Connection")
print("="*80)

# Show configuration (masked)
print(f"\nConfiguration:")
print(f"  DATABRICKS_HOST: {os.getenv('DATABRICKS_HOST')}")
print(f"  DATABRICKS_TOKEN: {'*' * 20 if os.getenv('DATABRICKS_TOKEN') else 'NOT SET'}")
print(f"  DATABRICKS_WAREHOUSE_ID: {os.getenv('DATABRICKS_WAREHOUSE_ID')}")

try:
    print("\n1. Connecting to Databricks...")
    config = Config(
        host=os.getenv("DATABRICKS_HOST"),
        token=os.getenv("DATABRICKS_TOKEN")
    )
    client = WorkspaceClient(config=config)
    
    print("✓ Connected!")
    
    print("\n2. Listing catalogs...")
    catalogs = list(client.catalogs.list())
    print(f"✓ Found {len(catalogs)} catalog(s):")
    
    for catalog in catalogs:
        print(f"\n  Catalog: {catalog.name}")
        print(f"    Owner: {catalog.owner}")
        print(f"    Comment: {catalog.comment or 'No description'}")
        
        # List schemas in this catalog
        print(f"    Schemas:")
        schemas = list(client.schemas.list(catalog_name=catalog.name))
        for schema in schemas[:3]:  # Show first 3
            print(f"      - {schema.name}")
        if len(schemas) > 3:
            print(f"      ... and {len(schemas) - 3} more")
    
    print("\n" + "="*80)
    print("✓ SUCCESS! Databricks connection works!")
    print("="*80)
    
except Exception as e:
    print(f"\n✗ ERROR: {e}")
    print("\nTroubleshooting:")
    print("1. Check your .env file exists and has correct values")
    print("2. Verify DATABRICKS_HOST includes https://")
    print("3. Verify DATABRICKS_TOKEN is a valid personal access token")
    print("4. Check you have Unity Catalog access in your workspace")
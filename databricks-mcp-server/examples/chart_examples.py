"""
Chart visualization examples for Databricks MCP Client
"""
import asyncio
import json
import base64
from pathlib import Path
from dotenv import load_dotenv
from databricks_mcp_server.client import DatabricksMCPClient

load_dotenv()


async def save_chart(chart_data: dict, filename: str):
    """Helper function to save chart image"""
    if 'image_data' in chart_data:
        img_bytes = base64.b64decode(chart_data['image_data'])
        output_path = Path(filename)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, 'wb') as f:
            f.write(img_bytes)

        print(f"  [OK] Chart saved to: {output_path}")
        return output_path
    elif 'error' in chart_data:
        print(f"  [ERROR] {chart_data['error']}")
        return None
    else:
        print(f"  [WARN] No image data in response: {json.dumps(chart_data, indent=2)}")
        return None


async def main():
    """Run chart visualization examples"""
    client = DatabricksMCPClient()

    async with client.connect():
        print("=" * 80)
        print("Databricks MCP Client - Chart Visualization Examples")
        print("=" * 80)

        # Configuration — uses the NYC Taxi trips sample dataset
        TABLE = "samples.nyctaxi.trips"

        print(f"\nUsing table: {TABLE}\n")

        # Example 1: Bar Chart — top 10 pickup zip codes by trip count
        print("\n1. Creating Bar Chart...")
        print("-" * 80)
        try:
            chart = await client.call_tool("create_chart", {
                "query": f"""
                    SELECT pickup_zip, COUNT(*) as trip_count
                    FROM {TABLE}
                    WHERE pickup_zip IS NOT NULL
                    GROUP BY pickup_zip
                    ORDER BY trip_count DESC
                    LIMIT 10
                """,
                "chart_type": "bar",
                "x_column": "pickup_zip",
                "y_column": "trip_count",
                "title": "Top 10 Pickup Zip Codes by Trip Count",
            })
            await save_chart(chart, "output/bar_chart.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")

        # Example 2: Line Chart — daily trip count over time
        print("\n2. Creating Line Chart (Time Series)...")
        print("-" * 80)
        try:
            chart = await client.call_tool("create_chart", {
                "query": f"""
                    SELECT DATE(tpep_pickup_datetime) as trip_date, COUNT(*) as trips
                    FROM {TABLE}
                    GROUP BY DATE(tpep_pickup_datetime)
                    ORDER BY trip_date
                    LIMIT 30
                """,
                "chart_type": "line",
                "x_column": "trip_date",
                "y_column": "trips",
                "title": "Daily Trip Count",
            })
            await save_chart(chart, "output/line_chart.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")

        # Example 3: Pie Chart — trip distribution by pickup zip (top 5)
        print("\n3. Creating Pie Chart...")
        print("-" * 80)
        try:
            chart = await client.call_tool("create_chart", {
                "query": f"""
                    SELECT CAST(pickup_zip AS STRING) as zip, COUNT(*) as trips
                    FROM {TABLE}
                    WHERE pickup_zip IS NOT NULL
                    GROUP BY pickup_zip
                    ORDER BY trips DESC
                    LIMIT 5
                """,
                "chart_type": "pie",
                "x_column": "zip",
                "y_column": "trips",
                "title": "Trip Distribution by Top 5 Pickup Zips",
            })
            await save_chart(chart, "output/pie_chart.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")

        # Example 4: Scatter Plot — trip distance vs fare amount
        print("\n4. Creating Scatter Plot...")
        print("-" * 80)
        try:
            chart = await client.call_tool("create_chart", {
                "query": f"""
                    SELECT trip_distance, fare_amount
                    FROM {TABLE}
                    WHERE trip_distance > 0 AND fare_amount > 0
                    LIMIT 500
                """,
                "chart_type": "scatter",
                "x_column": "trip_distance",
                "y_column": "fare_amount",
                "title": "Trip Distance vs Fare Amount",
            })
            await save_chart(chart, "output/scatter_plot.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")

        # Example 5: Histogram — fare amount distribution
        print("\n5. Creating Histogram...")
        print("-" * 80)
        try:
            chart = await client.call_tool("create_chart", {
                "query": f"""
                    SELECT fare_amount
                    FROM {TABLE}
                    WHERE fare_amount BETWEEN 0 AND 100
                    LIMIT 5000
                """,
                "chart_type": "histogram",
                "x_column": "fare_amount",
                "title": "Fare Amount Distribution",
            })
            await save_chart(chart, "output/histogram.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")

        # Example 6: Box Plot — trip distance distribution
        print("\n6. Creating Box Plot...")
        print("-" * 80)
        try:
            chart = await client.call_tool("create_chart", {
                "query": f"""
                    SELECT trip_distance
                    FROM {TABLE}
                    WHERE trip_distance BETWEEN 0 AND 30
                    LIMIT 5000
                """,
                "chart_type": "box",
                "y_column": "trip_distance",
                "title": "Trip Distance Distribution",
            })
            await save_chart(chart, "output/box_plot.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")

        print("\n" + "=" * 80)
        print("Chart visualization examples completed!")
        print("Charts saved to: ./output/")
        print("=" * 80)


async def dashboard_example():
    """Example: Create a dashboard with multiple charts"""
    client = DatabricksMCPClient()

    async with client.connect():
        print("=" * 80)
        print("Creating Dashboard with Multiple Charts")
        print("=" * 80)

        # Configuration — uses the NYC Taxi trips sample dataset
        TABLE = "samples.nyctaxi.trips"

        charts = [
            {
                "name": "trips_by_pickup_zip",
                "query": f"""
                    SELECT pickup_zip, COUNT(*) as trip_count
                    FROM {TABLE}
                    WHERE pickup_zip IS NOT NULL
                    GROUP BY pickup_zip
                    ORDER BY trip_count DESC
                    LIMIT 10
                """,
                "chart_type": "bar",
                "x_column": "pickup_zip",
                "y_column": "trip_count",
                "title": "Top 10 Pickup Zip Codes"
            },
            {
                "name": "daily_trips",
                "query": f"""
                    SELECT DATE(tpep_pickup_datetime) as trip_date, COUNT(*) as trips
                    FROM {TABLE}
                    GROUP BY DATE(tpep_pickup_datetime)
                    ORDER BY trip_date
                    LIMIT 30
                """,
                "chart_type": "line",
                "x_column": "trip_date",
                "y_column": "trips",
                "title": "Daily Trip Count (First 30 Days)"
            },
            {
                "name": "dropoff_zip_distribution",
                "query": f"""
                    SELECT CAST(dropoff_zip AS STRING) as zip, COUNT(*) as trips
                    FROM {TABLE}
                    WHERE dropoff_zip IS NOT NULL
                    GROUP BY dropoff_zip
                    ORDER BY trips DESC
                    LIMIT 5
                """,
                "chart_type": "pie",
                "x_column": "zip",
                "y_column": "trips",
                "title": "Trip Distribution by Top 5 Dropoff Zips"
            },
            {
                "name": "fare_distribution",
                "query": f"""
                    SELECT fare_amount
                    FROM {TABLE}
                    WHERE fare_amount BETWEEN 0 AND 100
                    LIMIT 5000
                """,
                "chart_type": "histogram",
                "x_column": "fare_amount",
                "title": "Fare Amount Distribution"
            }
        ]

        print(f"\nGenerating {len(charts)} charts for dashboard...\n")

        for i, chart_config in enumerate(charts, 1):
            print(f"{i}. {chart_config['title']}...")
            try:
                # Build arguments dict, excluding the helper 'name' key
                tool_args = {k: v for k, v in chart_config.items() if k != "name" and v is not None}
                chart = await client.call_tool("create_chart", tool_args)

                filename = f"output/dashboard_{chart_config['name']}.png"
                await save_chart(chart, filename)

            except Exception as e:
                print(f"  [ERROR] {e}")

        print("\n" + "=" * 80)
        print("Dashboard charts generated!")
        print("View all charts in: ./output/")
        print("=" * 80)


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--dashboard":
        asyncio.run(dashboard_example())
    else:
        asyncio.run(main())

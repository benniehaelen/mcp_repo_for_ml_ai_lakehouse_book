"""
Chart visualization examples for Databricks MCP Client
"""
import asyncio
import json
import base64
from pathlib import Path
from databricks_mcp_server.client import DatabricksMCPClient


async def save_chart(chart_data: dict, filename: str):
    """Helper function to save chart image"""
    if 'image_data' in chart_data:
        img_bytes = base64.b64decode(chart_data['image_data'])
        output_path = Path(filename)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_path, 'wb') as f:
            f.write(img_bytes)
        
        print(f"  ✓ Chart saved to: {output_path}")
        return output_path
    else:
        print("  ✗ No image data in response")
        return None


async def main():
    """Run chart visualization examples"""
    client = DatabricksMCPClient()
    
    async with client.connect():
        print("=" * 80)
        print("Databricks MCP Client - Chart Visualization Examples")
        print("=" * 80)
        
        # Configuration - update these with your actual data
        CATALOG = "main"
        SCHEMA = "default"
        TABLE = "your_table"
        
        print(f"\nUsing table: {CATALOG}.{SCHEMA}.{TABLE}")
        print("Note: Update the queries below to match your table schema\n")
        
        # Example 1: Bar Chart
        print("\n1. Creating Bar Chart...")
        print("-" * 80)
        try:
            chart = await client.create_chart(
                query=f"""
                    SELECT category, COUNT(*) as count 
                    FROM {CATALOG}.{SCHEMA}.{TABLE} 
                    GROUP BY category 
                    ORDER BY count DESC 
                    LIMIT 10
                """,
                chart_type="bar",
                x_column="category",
                y_column="count",
                title="Top 10 Categories"
            )
            await save_chart(chart, "output/bar_chart.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Example 2: Line Chart (Time Series)
        print("\n2. Creating Line Chart (Time Series)...")
        print("-" * 80)
        try:
            chart = await client.create_chart(
                query=f"""
                    SELECT DATE(timestamp) as date, COUNT(*) as records
                    FROM {CATALOG}.{SCHEMA}.{TABLE}
                    GROUP BY DATE(timestamp)
                    ORDER BY date
                """,
                chart_type="line",
                x_column="date",
                y_column="records",
                title="Daily Record Count"
            )
            await save_chart(chart, "output/line_chart.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Example 3: Pie Chart
        print("\n3. Creating Pie Chart...")
        print("-" * 80)
        try:
            chart = await client.create_chart(
                query=f"""
                    SELECT status, COUNT(*) as count
                    FROM {CATALOG}.{SCHEMA}.{TABLE}
                    GROUP BY status
                """,
                chart_type="pie",
                x_column="status",
                y_column="count",
                title="Distribution by Status"
            )
            await save_chart(chart, "output/pie_chart.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Example 4: Scatter Plot
        print("\n4. Creating Scatter Plot...")
        print("-" * 80)
        try:
            chart = await client.create_chart(
                query=f"""
                    SELECT metric1, metric2
                    FROM {CATALOG}.{SCHEMA}.{TABLE}
                    LIMIT 1000
                """,
                chart_type="scatter",
                x_column="metric1",
                y_column="metric2",
                title="Metric Correlation"
            )
            await save_chart(chart, "output/scatter_plot.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Example 5: Histogram
        print("\n5. Creating Histogram...")
        print("-" * 80)
        try:
            chart = await client.create_chart(
                query=f"""
                    SELECT value
                    FROM {CATALOG}.{SCHEMA}.{TABLE}
                    LIMIT 10000
                """,
                chart_type="histogram",
                x_column="value",
                title="Value Distribution"
            )
            await save_chart(chart, "output/histogram.png")
            print(f"  Message: {chart.get('message', 'Done')}")
        except Exception as e:
            print(f"  Error: {e}")
        
        # Example 6: Box Plot
        print("\n6. Creating Box Plot...")
        print("-" * 80)
        try:
            chart = await client.create_chart(
                query=f"""
                    SELECT revenue
                    FROM {CATALOG}.{SCHEMA}.{TABLE}
                    WHERE revenue IS NOT NULL
                """,
                chart_type="box",
                y_column="revenue",
                title="Revenue Distribution"
            )
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
        
        # Configuration
        CATALOG = "main"
        SCHEMA = "sales"
        
        charts = [
            {
                "name": "revenue_by_category",
                "query": f"""
                    SELECT category, SUM(revenue) as total_revenue
                    FROM {CATALOG}.{SCHEMA}.transactions
                    GROUP BY category
                    ORDER BY total_revenue DESC
                """,
                "type": "bar",
                "x": "category",
                "y": "total_revenue",
                "title": "Revenue by Category"
            },
            {
                "name": "daily_transactions",
                "query": f"""
                    SELECT DATE(transaction_date) as date, COUNT(*) as count
                    FROM {CATALOG}.{SCHEMA}.transactions
                    GROUP BY DATE(transaction_date)
                    ORDER BY date DESC
                    LIMIT 30
                """,
                "type": "line",
                "x": "date",
                "y": "count",
                "title": "Daily Transactions (Last 30 Days)"
            },
            {
                "name": "customer_segments",
                "query": f"""
                    SELECT segment, COUNT(DISTINCT customer_id) as customers
                    FROM {CATALOG}.{SCHEMA}.customers
                    GROUP BY segment
                """,
                "type": "pie",
                "x": "segment",
                "y": "customers",
                "title": "Customer Segments"
            },
            {
                "name": "order_value_distribution",
                "query": f"""
                    SELECT order_value
                    FROM {CATALOG}.{SCHEMA}.orders
                    WHERE order_value > 0
                """,
                "type": "histogram",
                "x": "order_value",
                "y": None,
                "title": "Order Value Distribution"
            }
        ]
        
        print(f"\nGenerating {len(charts)} charts for dashboard...\n")
        
        for i, chart_config in enumerate(charts, 1):
            print(f"{i}. {chart_config['title']}...")
            try:
                chart = await client.create_chart(
                    query=chart_config['query'],
                    chart_type=chart_config['type'],
                    x_column=chart_config['x'],
                    y_column=chart_config['y'],
                    title=chart_config['title']
                )
                
                filename = f"output/dashboard_{chart_config['name']}.png"
                await save_chart(chart, filename)
                
            except Exception as e:
                print(f"  ✗ Error: {e}")
        
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


from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SILVER = ROOT / "data_lake" / "silver"
GOLD = ROOT / "data_lake" / "gold"

GOLD.mkdir(parents=True, exist_ok=True)

# Load Silver tables
orders = pd.read_parquet(SILVER / "orders.parquet")
items = pd.read_parquet(SILVER / "order_items.parquet")

# Keep delivered orders only
delivered_orders = orders.loc[
    orders["order_status"].eq("DELIVERED")
].copy()

# Join order headers to their line items
sales = delivered_orders.merge(
    items,
    on="order_id",
    how="inner",
    validate="one_to_many",
)

# Calculate sales after line-level discounts
sales["net_line_sales"] = (
    sales["quantity"]
    * sales["unit_price"]
    * (1 - sales["discount_pct"] / 100)
)

# Aggregate by order date
gold_daily_sales = (
    sales.groupby("order_date", as_index=False)
    .agg(
        delivered_orders=("order_id", "nunique"),
        units_sold=("quantity", "sum"),
        net_sales=("net_line_sales", "sum"),
    )
    .sort_values("order_date")
)

gold_daily_sales["net_sales"] = (
    gold_daily_sales["net_sales"].round(2)
)

# Save Gold dataset
output_path = GOLD / "gold_daily_sales.parquet"
gold_daily_sales.to_parquet(output_path, index=False)

print("\n========== GOLD DAILY SALES REPORT ==========")
print(f"Delivered orders: {delivered_orders['order_id'].nunique()}")
print(f"Matched order-item rows: {len(sales)}")
print(f"Sales days: {len(gold_daily_sales)}")
print(f"Total units sold: {gold_daily_sales['units_sold'].sum()}")
print(f"Total net sales: {gold_daily_sales['net_sales'].sum():,.2f}")

print("\nSample output:")
print(gold_daily_sales.head(10).to_string(index=False))

print(f"\nGold saved to: {output_path}")

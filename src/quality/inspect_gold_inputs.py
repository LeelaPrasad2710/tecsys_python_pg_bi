
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SILVER = ROOT / "data_lake" / "silver"

orders = pd.read_parquet(SILVER / "orders.parquet")
items = pd.read_parquet(SILVER / "order_items.parquet")

print("\n========== ORDER STATUS COUNTS ==========")
print(orders["order_status"].value_counts(dropna=False))

print("\n========== ORDER ITEM SUMMARY ==========")
print(items[["quantity", "unit_price", "discount_pct"]].describe())

print("\n========== DISCOUNT VALUES ==========")
print(items["discount_pct"].value_counts(dropna=False).head(15))

print("\n========== MISSING VALUES ==========")
print("Orders:")
print(orders[["order_id", "order_date", "order_status"]].isna().sum())

print("\nOrder items:")
print(items[["order_id", "quantity", "unit_price", "discount_pct"]].isna().sum())

print("\n========== GOLD INPUT COUNTS ==========")
print(f"Silver orders: {len(orders)}")
print(f"Silver order items: {len(items)}")
print(f"Unique orders in order items: {items['order_id'].nunique()}")

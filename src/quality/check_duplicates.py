
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"

orders = pd.read_parquet(BRONZE_DIR / "orders.parquet")

duplicate_count = orders["order_id"].duplicated().sum()
unique_order_ids = orders["order_id"].nunique()

print("Total rows:", len(orders))
print("Unique order IDs:", unique_order_ids)
print("Duplicate occurrences:", duplicate_count)

duplicate_rows = orders[
    orders["order_id"].duplicated(keep=False)
].sort_values("order_id")

print("\nDuplicate order records:")
print(duplicate_rows.to_string(index=False))

print("\nMissing customer IDs:")
print(orders["customer_id"].isna().sum())

missing_customers = orders[
    orders["customer_id"].isna()
]

print("\nOrders with missing customer IDs:")
print(missing_customers.to_string(index=False))
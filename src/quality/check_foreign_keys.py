
from pathlib import Path
import pandas as pd

# 1. Find project folders
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"

# 2. Read both Parquet datasets
orders = pd.read_parquet(BRONZE_DIR / "orders.parquet")
customers = pd.read_parquet(BRONZE_DIR / "customers.parquet")

# 3. Get valid customer IDs from the customers table
valid_customer_ids = customers["customer_id"].dropna()

# 4. Identify orders with missing customer IDs separately
missing_customer_rows = orders[
    orders["customer_id"].isna()
]

# 5. Find non-null customer IDs that don't exist in customers
orphan_rows = orders[
    orders["customer_id"].notna()
    & ~orders["customer_id"].isin(valid_customer_ids)
]

# 6. Display the results
print("Total order rows:", len(orders))
print("Missing customer ID rows:", len(missing_customer_rows))
print("Orphan customer reference rows:", len(orphan_rows))

print("\nOrphan orders:")
print(orphan_rows.to_string(index=False))

print("\nUnique orphan customer IDs:")
print(orphan_rows["customer_id"].unique())

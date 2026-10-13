
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"

tables = {
    "customers": ("customers.parquet", "customer_id"),
    "products": ("products.parquet", "product_id"),
}

for table_name, (filename, key) in tables.items():
    df = pd.read_parquet(BRONZE_DIR / filename)

    duplicate_mask = (
        df[key].notna()
        & df[key].duplicated(keep=False)
    )

    duplicates = df.loc[duplicate_mask].sort_values(key)

    print(f"\n========== {table_name.upper()} DUPLICATES ==========")
    print("Rows involved in duplicate keys:", len(duplicates))
    print(duplicates.to_string(index=False))

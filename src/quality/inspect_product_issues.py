
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"

products = pd.read_parquet(BRONZE_DIR / "products.parquet")

missing_category = products["category"].isna()

negative_price = (
    products["unit_price"].notna()
    & (products["unit_price"] < 0)
)

print("Products with missing category:", int(missing_category.sum()))
print("Products with negative price:", int(negative_price.sum()))

print("\nProducts with missing categories:")
print(products.loc[missing_category].to_string(index=False))

print("\nProducts with negative prices:")
print(products.loc[negative_price].to_string(index=False))

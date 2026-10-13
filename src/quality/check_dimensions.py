
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"

customers = pd.read_parquet(BRONZE_DIR / "customers.parquet")
products = pd.read_parquet(BRONZE_DIR / "products.parquet")


def check_dimension(df, table_name, primary_key):
    print(f"\n========== {table_name.upper()} ==========")
    print("Total rows:", len(df))
    print("Missing primary keys:", int(df[primary_key].isna().sum()))
    print(
        "Duplicate primary-key occurrences:",
        int(df.loc[df[primary_key].notna(), primary_key].duplicated().sum())
    )

    if table_name == "products":
        print("Missing product names:", int(df["product_name"].isna().sum()))
        print("Missing categories:", int(df["category"].isna().sum()))
        print("Missing unit prices:", int(df["unit_price"].isna().sum()))
        print(
            "Negative unit prices:",
            int((df["unit_price"] < 0).sum())
        )


check_dimension(customers, "customers", "customer_id")
check_dimension(products, "products", "product_id")

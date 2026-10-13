
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
GOLD = ROOT / "data_lake" / "gold"

files = [
    "gold_daily_sales.parquet",
    "gold_product_performance.parquet",
    "gold_delivery_performance.parquet",
    "gold_customer_analytics.parquet",
    "gold_returns_analytics.parquet",
]

for filename in files:
    df = pd.read_parquet(GOLD / filename)

    print(f"\n========== {filename} ==========")
    print("Columns:", df.columns.tolist())
    print("Rows:", len(df))

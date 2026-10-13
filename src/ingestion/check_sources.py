
from pathlib import Path
import pandas as pd

# Locate the project root from this Python file
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Build the path to the raw data folder
RAW_DIR = PROJECT_ROOT / "data" / "raw"

source_files = [
    "customers.csv",
    "products.csv",
    "orders.csv",
    "order_items.csv",
    "deliveries.csv",
    "returns.csv",
]

for filename in source_files:
    file_path = RAW_DIR / filename

    if not file_path.exists():
        print(f"MISSING: {filename}")
        continue

    df = pd.read_csv(file_path)

    print(f"\nFile: {filename}")
    print(f"Rows and columns: {df.shape}")
    print(f"Columns: {list(df.columns)}")

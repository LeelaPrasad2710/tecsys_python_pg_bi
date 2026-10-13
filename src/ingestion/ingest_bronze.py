
from pathlib import Path
import pandas as pd

# Find the project root
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# Define source and destination folders
RAW_DIR = PROJECT_ROOT / "data" / "raw"
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"

# Create the Bronze folder if it doesn't exist
BRONZE_DIR.mkdir(parents=True, exist_ok=True)

source_files = [
    "customers.csv",
    "products.csv",
    "orders.csv",
    "order_items.csv",
    "deliveries.csv",
    "returns.csv",
]

for filename in source_files:
    source_path = RAW_DIR / filename

    if not source_path.exists():
        raise FileNotFoundError(
            f"Required source file is missing: {source_path}"
        )

    # Step 1: Read CSV into memory
    df = pd.read_csv(source_path)

    # Step 2: Define the Parquet output path
    output_path = BRONZE_DIR / f"{source_path.stem}.parquet"

    # Step 3: Persist the DataFrame to disk
    df.to_parquet(output_path, index=False)

    print(
        f"Ingested {filename}: {len(df)} rows "
        f"-> {output_path.name}"
    )

print("Bronze ingestion completed.")

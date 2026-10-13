
from pathlib import Path
import os
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# --------------------------------------------------
# 1. Project paths
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]

GOLD_DIR = ROOT / "data_lake" / "gold"

SOURCE_FILE = GOLD_DIR / "gold_daily_sales.parquet"

# --------------------------------------------------
# 2. Database connection settings
# --------------------------------------------------
# Set these environment variables in your terminal.
# Do not hardcode your database password in this file.
DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "tecsys_analytics")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASSWORD = os.getenv("PGPASSWORD")

if not DB_PASSWORD:
    raise ValueError(
        "PGPASSWORD is not set. Set it in your terminal first."
    )

url = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=int(DB_PORT),
    database=DB_NAME,
)

engine = create_engine(url)

# --------------------------------------------------
# 3. Read the Gold dataset
# --------------------------------------------------
df = pd.read_parquet(SOURCE_FILE)

print(f"Source file: {SOURCE_FILE}")
print(f"Source rows: {len(df)}")
print(f"Columns: {df.columns.tolist()}")

# --------------------------------------------------
# 4. Load into PostgreSQL
# --------------------------------------------------
try:
    with engine.begin() as connection:
        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS analytics")
        )

        df.to_sql(
            name="gold_daily_sales",
            con=connection,
            schema="analytics",
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi",
        )

    print("\nGold Daily Sales loaded successfully.")

    # --------------------------------------------------
    # 5. Verify the database table
    # --------------------------------------------------
    with engine.connect() as connection:
        row_count = connection.execute(
            text("SELECT COUNT(*) FROM analytics.gold_daily_sales")
        ).scalar_one()

        print(f"PostgreSQL rows: {row_count}")

        if row_count != len(df):
            raise ValueError(
                f"Row-count mismatch: source={len(df)}, "
                f"PostgreSQL={row_count}"
            )

        print("Row-count reconciliation: PASS")

finally:
    engine.dispose()

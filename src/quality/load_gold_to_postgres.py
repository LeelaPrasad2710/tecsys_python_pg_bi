
from pathlib import Path
import os
import sys

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# ============================================================
# 1. PROJECT CONFIGURATION
# ============================================================

ROOT = Path(__file__).resolve().parents[2]
GOLD_DIR = ROOT / "data_lake" / "gold"

SCHEMA = "analytics"

GOLD_TABLES = {
    "gold_daily_sales.parquet": "gold_daily_sales",
    "gold_product_performance.parquet": "gold_product_performance",
    "gold_delivery_performance.parquet": "gold_delivery_performance",
    "gold_customer_analytics.parquet": "gold_customer_analytics",
    "gold_returns_analytics.parquet": "gold_returns_analytics",
}


# ============================================================
# 2. POSTGRESQL CONNECTION
# ============================================================

DB_HOST = os.getenv("PGHOST", "localhost")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "tecsys_analytics")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASSWORD = os.getenv("PGPASSWORD")

if not DB_PASSWORD:
    print("ERROR: PGPASSWORD is not set.")
    print("Set your PostgreSQL password in PowerShell and retry.")
    sys.exit(1)

url = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=int(DB_PORT),
    database=DB_NAME,
)

engine = create_engine(
    url,
    pool_pre_ping=True,
)


# ============================================================
# 3. LOAD AND VALIDATE GOLD SOURCE FILES
# ============================================================

def load_source_datasets():
    datasets = {}

    print("========== GOLD SOURCE SUMMARY ==========")

    for filename, table_name in GOLD_TABLES.items():
        source_path = GOLD_DIR / filename

        if not source_path.is_file():
            raise FileNotFoundError(
                f"Gold source file not found: {source_path}"
            )

        df = pd.read_parquet(source_path)

        if len(df.columns) == 0:
            raise ValueError(
                f"No columns found in source file: {filename}"
            )

        if not df.columns.is_unique:
            raise ValueError(
                f"Duplicate column names found in: {filename}"
            )

        if df.columns.isna().any():
            raise ValueError(
                f"Missing column names found in: {filename}"
            )

        datasets[table_name] = df

        print(
            f"{table_name}: {len(df)} rows, "
            f"{len(df.columns)} columns"
        )

    return datasets


# ============================================================
# 4. ATOMIC POSTGRESQL REFRESH
# ============================================================

def refresh_gold_tables(datasets):
    """
    Refresh all Gold tables in one database transaction.

    Existing tables are truncated and reloaded instead of
    being dropped and recreated. If a load or row-count check
    fails, the transaction rolls back.
    """

    expected_table_names = set(GOLD_TABLES.values())

    if set(datasets.keys()) != expected_table_names:
        raise ValueError(
            "The loaded datasets do not match the configured "
            "Gold table list."
        )

    with engine.begin() as connection:

        # Create the schema if it does not already exist.
        connection.execute(
            text(f'CREATE SCHEMA IF NOT EXISTS "{SCHEMA}"')
        )

        # Check that existing tables have compatible columns.
        # Fail safely instead of dropping tables to change schemas.
        for table_name, df in datasets.items():

            table_exists = connection.execute(
                text("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = :schema
                          AND table_name = :table_name
                    )
                """),
                {
                    "schema": SCHEMA,
                    "table_name": table_name,
                },
            ).scalar_one()

            if table_exists:
                existing_columns = connection.execute(
                    text("""
                        SELECT column_name
                        FROM information_schema.columns
                        WHERE table_schema = :schema
                          AND table_name = :table_name
                        ORDER BY ordinal_position
                    """),
                    {
                        "schema": SCHEMA,
                        "table_name": table_name,
                    },
                ).scalars().all()

                source_columns = list(df.columns)

                if existing_columns != source_columns:
                    raise ValueError(
                        f"Schema mismatch for {SCHEMA}.{table_name}.\n"
                        f"Database columns: {existing_columns}\n"
                        f"Source columns: {source_columns}\n"
                        "No tables were refreshed. Review the schema "
                        "before applying a migration."
                    )

        # All validation above happens before any data is replaced.
        print("\n========== REFRESHING GOLD TABLES ==========")

        # TRUNCATE is transactional in PostgreSQL.
        # RESTRICT avoids silently cascading into dependent tables.
        for table_name in datasets:
            table_exists = connection.execute(
                text("""
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = :schema
                          AND table_name = :table_name
                    )
                """),
                {
                    "schema": SCHEMA,
                    "table_name": table_name,
                },
            ).scalar_one()

            if table_exists:
                print(f"Clearing existing table: {SCHEMA}.{table_name}")

                connection.execute(
                    text(
                        f'TRUNCATE TABLE "{SCHEMA}"."{table_name}" '
                        "RESTRICT"
                    )
                )

        # Load each source dataset.
        for table_name, df in datasets.items():

            print(f"\nLoading {SCHEMA}.{table_name}...")

            # append works for both newly created and truncated tables.
            df.to_sql(
                name=table_name,
                con=connection,
                schema=SCHEMA,
                if_exists="append",
                index=False,
                chunksize=1000,
                method="multi",
            )

            expected_rows = len(df)

            actual_rows = connection.execute(
                text(
                    f'SELECT COUNT(*) '
                    f'FROM "{SCHEMA}"."{table_name}"'
                )
            ).scalar_one()

            print(f"Source rows: {expected_rows}")
            print(f"PostgreSQL rows: {actual_rows}")

            if actual_rows != expected_rows:
                raise ValueError(
                    f"Row-count mismatch for {table_name}: "
                    f"source={expected_rows}, "
                    f"database={actual_rows}"
                )

            print(f"{table_name}: PASS")

        # Explicitly verify the entire set before commit.
        print("\n========== FINAL ROW-COUNT VERIFICATION ==========")

        for table_name, df in datasets.items():
            actual_rows = connection.execute(
                text(
                    f'SELECT COUNT(*) '
                    f'FROM "{SCHEMA}"."{table_name}"'
                )
            ).scalar_one()

            expected_rows = len(df)

            if actual_rows != expected_rows:
                raise ValueError(
                    f"Final verification failed for {table_name}: "
                    f"expected={expected_rows}, actual={actual_rows}"
                )

            print(
                f"{table_name}: PASS "
                f"({actual_rows} rows)"
            )

    # Reaching this line means engine.begin() committed successfully.
    print("\n========== LOAD SUMMARY ==========")
    print(f"Tables loaded successfully: {len(datasets)}")
    print(
        f"Total rows loaded: "
        f"{sum(len(df) for df in datasets.values()):,}"
    )
    print("All row counts reconciled.")
    print("Transaction committed successfully.")


# ============================================================
# 5. MAIN ENTRY POINT
# ============================================================

def main():
    try:
        datasets = load_source_datasets()
        refresh_gold_tables(datasets)
        return 0

    except Exception as exc:
        print("\n❌ GOLD LOAD FAILED")
        print(f"Reason: {exc}")
        print("The database transaction was rolled back if it had begun.")
        return 1

    finally:
        engine.dispose()


if __name__ == "__main__":
    raise SystemExit(main())

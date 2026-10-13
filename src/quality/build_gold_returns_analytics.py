
from pathlib import Path
import pandas as pd

# --------------------------------------------------
# 1. Define project paths
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = ROOT / "data_lake" / "silver"
GOLD_DIR = ROOT / "data_lake" / "gold"

GOLD_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# 2. Load Silver Returns
# --------------------------------------------------
returns = pd.read_parquet(
    SILVER_DIR / "returns.parquet"
)

print("========== SOURCE SUMMARY ==========")
print(f"Silver return records: {len(returns)}")


# --------------------------------------------------
# 3. Prepare columns
# --------------------------------------------------
returns["return_date"] = pd.to_datetime(
    returns["return_date"],
    errors="coerce"
)

returns["return_quantity"] = pd.to_numeric(
    returns["return_quantity"],
    errors="coerce"
)

# Defensive validation: Silver should already be clean.
if returns["return_id"].isna().any():
    raise ValueError("Silver Returns contains missing return IDs.")

if returns["return_id"].duplicated().any():
    raise ValueError("Silver Returns contains duplicate return IDs.")

if returns["order_id"].isna().any():
    raise ValueError("Silver Returns contains missing order IDs.")

if returns["return_date"].isna().any():
    raise ValueError("Silver Returns contains invalid return dates.")

if returns["return_reason"].isna().any():
    raise ValueError("Silver Returns contains missing return reasons.")

if (
    returns["return_quantity"].isna().any()
    or (returns["return_quantity"] <= 0).any()
):
    raise ValueError("Silver Returns contains invalid return quantities.")


# --------------------------------------------------
# 4. Aggregate by return reason
# --------------------------------------------------
gold = (
    returns
    .groupby("return_reason", as_index=False)
    .agg(
        return_count=("return_id", "nunique"),
        orders_with_returns=("order_id", "nunique"),
        returned_quantity=("return_quantity", "sum")
    )
)

# Highest return volume gets rank 1.
gold["return_reason_rank"] = (
    gold["return_count"]
    .rank(method="dense", ascending=False)
    .astype("int64")
)

gold = gold.sort_values(
    ["return_count", "return_reason"],
    ascending=[False, True]
).reset_index(drop=True)


# --------------------------------------------------
# 5. Save Gold dataset
# --------------------------------------------------
output_path = GOLD_DIR / "gold_returns_analytics.parquet"

gold.to_parquet(output_path, index=False)


# --------------------------------------------------
# 6. Print summary
# --------------------------------------------------
print("\n========== GOLD RETURNS ANALYTICS ==========")
print(f"Distinct return reasons: {gold['return_reason'].nunique()}")
print(f"Gold rows: {len(gold)}")
print(f"Total return records: {gold['return_count'].sum()}")
print(f"Total returned quantity: {gold['returned_quantity'].sum()}")
print(f"Distinct orders with returns: {returns['order_id'].nunique()}")

print("\nReturns by reason:")
print(gold.to_string(index=False))

print(f"\nSaved Gold dataset: {output_path}")

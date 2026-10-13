
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SILVER_DIR = ROOT / "data_lake" / "silver"
GOLD_DIR = ROOT / "data_lake" / "gold"

returns = pd.read_parquet(SILVER_DIR / "returns.parquet")
gold = pd.read_parquet(GOLD_DIR / "gold_returns_analytics.parquet")

# Independently rebuild the expected aggregation.
expected = (
    returns
    .groupby("return_reason", as_index=False)
    .agg(
        expected_return_count=("return_id", "nunique"),
        expected_orders_with_returns=("order_id", "nunique"),
        expected_returned_quantity=("return_quantity", "sum")
    )
)

actual = gold.merge(
    expected,
    on="return_reason",
    how="outer",
    indicator=True,
    validate="one_to_one"
)

checks = {
    "One row per return reason": (
        gold["return_reason"].notna().all()
        and gold["return_reason"].is_unique
        and len(gold) == returns["return_reason"].nunique()
    ),
    "All return reasons reconcile": actual["_merge"].eq("both").all(),
    "Return counts reconcile": (
        actual["return_count"]
        .eq(actual["expected_return_count"]).all()
    ),
    "Orders per reason reconcile": (
        actual["orders_with_returns"]
        .eq(actual["expected_orders_with_returns"]).all()
    ),
    "Returned quantities reconcile": (
        actual["returned_quantity"]
        .eq(actual["expected_returned_quantity"]).all()
    ),
    "Total return count reconciles": (
        gold["return_count"].sum() == len(returns)
    ),
    "Total returned quantity reconciles": (
        gold["returned_quantity"].sum()
        == returns["return_quantity"].sum()
    ),
    "Return ranks are correct": (
        gold["return_reason_rank"].eq(
            gold["return_count"].rank(
                method="dense", ascending=False
            ).astype("int64")
        ).all()
    ),
}

print("========== GOLD RETURNS VALIDATION ==========")

for name, passed in checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")

print(f"\nPassed: {sum(checks.values())}/{len(checks)}")

# Report questionable reason values separately.
print("\n========== REASON QUALITY REVIEW ==========")
print(
    returns["return_reason"]
    .value_counts(dropna=False)
    .to_string()
)

if not all(checks.values()):
    raise SystemExit("Gold Returns validation failed.")

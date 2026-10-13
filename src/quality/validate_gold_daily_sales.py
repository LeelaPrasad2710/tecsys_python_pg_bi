
from pathlib import Path
import pandas as pd

# ==================================================
# 1. PROJECT PATHS
# ==================================================
ROOT = Path(__file__).resolve().parents[2]

SILVER = ROOT / "data_lake" / "silver"
GOLD = ROOT / "data_lake" / "gold"

GOLD_FILE = GOLD / "gold_daily_sales.parquet"

# ==================================================
# 2. LOAD SILVER AND GOLD DATA
# ==================================================
orders = pd.read_parquet(SILVER / "orders.parquet")
items = pd.read_parquet(SILVER / "order_items.parquet")
gold = pd.read_parquet(GOLD_FILE)

# ==================================================
# 3. REBUILD EXPECTED SALES INDEPENDENTLY
# ==================================================
delivered_orders = orders.loc[
    orders["order_status"].eq("DELIVERED")
].copy()

sales = delivered_orders.merge(
    items,
    on="order_id",
    how="inner",
    validate="one_to_many",
)

sales["expected_net_sales"] = (
    sales["quantity"]
    * sales["unit_price"]
    * (1 - sales["discount_pct"] / 100)
)

# ==================================================
# 4. AGGREGATE EXPECTED RESULTS BY DATE
# ==================================================
expected_daily_sales = (
    sales.groupby("order_date", as_index=False)
    .agg(
        delivered_orders=("order_id", "nunique"),
        units_sold=("quantity", "sum"),
        net_sales=("expected_net_sales", "sum"),
    )
    .sort_values("order_date")
    .reset_index(drop=True)
)

# Match the Gold transformation's daily rounding rule
expected_daily_sales["net_sales"] = (
    expected_daily_sales["net_sales"].round(2)
)

gold = (
    gold.sort_values("order_date")
    .reset_index(drop=True)
)

# ==================================================
# 5. VALIDATION CHECKS
# ==================================================

# Check 1: One row per date
check_unique_dates = gold["order_date"].is_unique

# Check 2: No missing dates
check_no_missing_dates = gold["order_date"].notna().all()

# Check 3: No negative units
check_no_negative_units = (
    gold["units_sold"].ge(0).all()
)

# Check 4: No negative sales
check_no_negative_sales = (
    gold["net_sales"].ge(0).all()
)

# Check 5: Delivered order counts reconcile
expected_order_count = delivered_orders["order_id"].nunique()
actual_order_count = gold["delivered_orders"].sum()

check_order_count = (
    actual_order_count == expected_order_count
)

# Check 6: Units sold reconcile
expected_units = sales["quantity"].sum()
actual_units = gold["units_sold"].sum()

check_units = actual_units == expected_units

# Check 7: Sales reconcile at the daily aggregation level
same_dates = (
    gold["order_date"].reset_index(drop=True).equals(
        expected_daily_sales["order_date"].reset_index(drop=True)
    )
)

if same_dates:
    differences = (
        gold["net_sales"].reset_index(drop=True)
        - expected_daily_sales["net_sales"].reset_index(drop=True)
    ).abs()

    max_daily_difference = (
        differences.max() if len(differences) else 0
    )

    check_sales = max_daily_difference < 0.001
else:
    max_daily_difference = None
    check_sales = False

# ==================================================
# 6. PRINT RECONCILIATION DETAILS
# ==================================================
print("\n========== SALES RECONCILIATION DETAILS ==========")

print(f"Delivered orders: {expected_order_count}")
print(f"Expected units sold: {expected_units}")
print(f"Actual units sold: {actual_units}")

print(
    f"Expected net sales: "
    f"{expected_daily_sales['net_sales'].sum():,.2f}"
)
print(
    f"Actual Gold net sales: "
    f"{gold['net_sales'].sum():,.2f}"
)

print(
    f"Total sales difference: "
    f"{gold['net_sales'].sum() - expected_daily_sales['net_sales'].sum():,.2f}"
)

if max_daily_difference is not None:
    print(f"Maximum daily difference: {max_daily_difference:.4f}")

# ==================================================
# 7. DISPLAY ALL VALIDATION RESULTS
# ==================================================
checks = {
    "One row per date": check_unique_dates,
    "No missing dates": check_no_missing_dates,
    "No negative units": check_no_negative_units,
    "No negative sales": check_no_negative_sales,
    "Order count reconciles": check_order_count,
    "Units reconcile": check_units,
    "Sales reconcile": check_sales,
}

print("\n========== GOLD VALIDATION ==========")

for check_name, passed in checks.items():
    status = "PASS" if passed else "FAIL"
    print(f"{check_name}: {status}")

passed_count = sum(checks.values())
total_checks = len(checks)

print(f"\nPassed: {passed_count}/{total_checks}")

if passed_count == total_checks:
    print("All Gold daily sales validations passed.")
else:
    print("Some validations failed. Investigate the details above.")

print(f"\nGold file validated: {GOLD_FILE}")

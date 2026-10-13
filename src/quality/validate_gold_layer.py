
from pathlib import Path
import pandas as pd
import sys

# ============================================================
# 1. PROJECT PATHS AND HELPER FUNCTION
# ============================================================

ROOT = Path(__file__).resolve().parents[2]

SILVER = ROOT / "data_lake" / "silver"
GOLD = ROOT / "data_lake" / "gold"
REPORTS = ROOT / "reports"

REPORTS.mkdir(parents=True, exist_ok=True)

TOLERANCE = 1.00
checks = {}


def add_check(name, passed, detail=""):
    """Record a validation check and print its result."""

    checks[name] = bool(passed)

    status = "PASS" if passed else "FAIL"
    suffix = f" | {detail}" if detail else ""

    print(f"{name}: {status}{suffix}")


# ============================================================
# 2. LOAD DATASETS
# ============================================================

def load_parquet(path):
    """Load a Parquet file or stop validation if it is missing."""

    if not path.exists():
        print(f"\nERROR: Required Parquet file not found: {path}")
        sys.exit(1)

    try:
        return pd.read_parquet(path)
    except Exception as exc:
        print(f"\nERROR: Could not read Parquet file: {path}")
        print(f"Reason: {exc}")
        sys.exit(1)


customers = load_parquet(SILVER / "customers.parquet")
orders = load_parquet(SILVER / "orders.parquet")
items = load_parquet(SILVER / "order_items.parquet")
deliveries = load_parquet(SILVER / "deliveries.parquet")
returns = load_parquet(SILVER / "returns.parquet")

daily = load_parquet(GOLD / "gold_daily_sales.parquet")
product = load_parquet(
    GOLD / "gold_product_performance.parquet"
)
delivery_gold = load_parquet(
    GOLD / "gold_delivery_performance.parquet"
)
customer_gold = load_parquet(
    GOLD / "gold_customer_analytics.parquet"
)
returns_gold = load_parquet(
    GOLD / "gold_returns_analytics.parquet"
)

print("========== GOLD LAYER VALIDATION ==========")


# ============================================================
# 3. INDEPENDENTLY CALCULATE DELIVERED SALES
# ============================================================

delivered_orders = orders.loc[
    orders["order_status"].eq("DELIVERED"),
    ["order_id", "customer_id"],
].copy()

delivered_items = items.merge(
    delivered_orders[["order_id"]],
    on="order_id",
    how="inner",
    validate="many_to_one",
)

for col in ["quantity", "unit_price", "discount_pct"]:
    delivered_items[col] = pd.to_numeric(
        delivered_items[col],
        errors="coerce",
    )

delivered_items["discount_pct"] = (
    delivered_items["discount_pct"].fillna(0)
)

delivered_items["expected_sales"] = (
    delivered_items["quantity"]
    * delivered_items["unit_price"]
    * (1 - delivered_items["discount_pct"] / 100)
)

expected_sales = round(
    delivered_items["expected_sales"].sum(),
    2,
)

expected_units = int(
    delivered_items["quantity"].sum()
)

expected_orders = int(
    delivered_orders["order_id"].nunique()
)

daily_sales = float(daily["net_sales"].sum())
daily_units = int(daily["units_sold"].sum())
daily_orders = int(daily["delivered_orders"].sum())

print("\n--- Daily Sales ---")

add_check(
    "Daily Sales units reconcile to Silver",
    daily_units == expected_units,
    f"Gold={daily_units}, expected={expected_units}",
)

add_check(
    "Daily Sales revenue reconciles to Silver",
    abs(daily_sales - expected_sales) <= TOLERANCE,
    f"Gold={daily_sales:.2f}, expected={expected_sales:.2f}",
)

add_check(
    "Daily Sales dates are unique",
    daily["order_date"].is_unique,
)

add_check(
    "Daily delivered order count reconciles",
    daily_orders == expected_orders,
    f"Gold={daily_orders}, expected={expected_orders}",
)


# ============================================================
# 4. PRODUCT PERFORMANCE
# ============================================================

product_sales = float(product["net_sales"].sum())
product_units = int(product["units_sold"].sum())

print("\n--- Product Performance ---")

add_check(
    "Product IDs are unique",
    product["product_id"].is_unique,
)

add_check(
    "Product units reconcile to Daily Sales",
    product_units == daily_units,
    f"Product={product_units}, Daily={daily_units}",
)

add_check(
    "Product sales reconcile to Daily Sales",
    abs(product_sales - daily_sales) <= TOLERANCE,
    f"Product={product_sales:.2f}, Daily={daily_sales:.2f}",
)

add_check(
    "Product revenue ranks are correct",
    product["revenue_rank"].eq(
        product["net_sales"]
        .rank(method="dense", ascending=False)
        .astype("int64")
    ).all(),
)


# ============================================================
# 5. CUSTOMER ANALYTICS
# ============================================================

customer_sales = float(customer_gold["net_sales"].sum())

customer_units = int(
    customer_gold["units_purchased"].sum()
)

customer_order_count = int(
    customer_gold["delivered_order_count"].sum()
)

print("\n--- Customer Analytics ---")

add_check(
    "All Silver customers represented",
    len(customer_gold) == len(customers)
    and customer_gold["customer_id"].is_unique,
)

add_check(
    "Customer units reconcile to Daily Sales",
    customer_units == daily_units,
    f"Customer={customer_units}, Daily={daily_units}",
)

add_check(
    "Customer sales reconcile to Daily Sales",
    abs(customer_sales - daily_sales) <= TOLERANCE,
    f"Customer={customer_sales:.2f}, Daily={daily_sales:.2f}",
)

add_check(
    "Customer delivered orders reconcile",
    customer_order_count == expected_orders,
    f"Customer={customer_order_count}, expected={expected_orders}",
)


# ============================================================
# 6. DELIVERY PERFORMANCE
# ============================================================

eligible = deliveries.loc[
    deliveries["delivery_status"].eq("DELIVERED")
].copy()

for col in [
    "dispatch_date",
    "expected_delivery_date",
    "actual_delivery_date",
]:
    eligible[col] = pd.to_datetime(
        eligible[col],
        errors="coerce",
    )

eligible = eligible.loc[
    eligible["dispatch_date"].notna()
    & eligible["expected_delivery_date"].notna()
    & eligible["actual_delivery_date"].notna()
    & (
        eligible["actual_delivery_date"]
        >= eligible["dispatch_date"]
    )
].copy()

expected_eligible = len(eligible)

expected_on_time = int(
    (
        eligible["actual_delivery_date"]
        <= eligible["expected_delivery_date"]
    ).sum()
)

expected_late = expected_eligible - expected_on_time

expected_rate = (
    round(
        expected_on_time / expected_eligible * 100,
        2,
    )
    if expected_eligible
    else 0
)

expected_duration = (
    round(
        (
            eligible["actual_delivery_date"]
            - eligible["dispatch_date"]
        ).dt.days.mean(),
        2,
    )
    if expected_eligible
    else 0
)

print("\n--- Delivery Performance ---")

if len(delivery_gold) == 1:
    d = delivery_gold.iloc[0]

    add_check(
        "Delivery Gold has one KPI row",
        True,
    )

    add_check(
        "Eligible delivery count reconciles",
        int(d["eligible_deliveries"]) == expected_eligible,
        f"Gold={d['eligible_deliveries']}, expected={expected_eligible}",
    )

    add_check(
        "On-time delivery count reconciles",
        int(d["on_time_deliveries"]) == expected_on_time,
        f"Gold={d['on_time_deliveries']}, expected={expected_on_time}",
    )

    add_check(
        "Late delivery count reconciles",
        int(d["late_deliveries"]) == expected_late,
        f"Gold={d['late_deliveries']}, expected={expected_late}",
    )

    add_check(
        "On-time delivery rate reconciles",
        abs(
            float(d["on_time_delivery_pct"]) - expected_rate
        ) < 0.01,
        f"Gold={d['on_time_delivery_pct']}, expected={expected_rate}",
    )

    add_check(
        "Average delivery duration reconciles",
        abs(
            float(d["average_delivery_duration_days"])
            - expected_duration
        ) < 0.01,
        f"Gold={d['average_delivery_duration_days']}, "
        f"expected={expected_duration}",
    )

else:
    add_check(
        "Delivery Gold has one KPI row",
        False,
        f"Found {len(delivery_gold)} rows",
    )


# ============================================================
# 7. RETURNS ANALYTICS
# ============================================================

expected_returns = (
    returns.groupby("return_reason", as_index=False)
    .agg(
        expected_return_count=("return_id", "nunique"),
        expected_order_count=("order_id", "nunique"),
        expected_quantity=("return_quantity", "sum"),
    )
)

returns_comparison = returns_gold.merge(
    expected_returns,
    on="return_reason",
    how="outer",
    indicator=True,
    validate="one_to_one",
)

print("\n--- Returns Analytics ---")

add_check(
    "Return reasons reconcile",
    returns_comparison["_merge"].eq("both").all(),
)

add_check(
    "Return counts reconcile",
    returns_comparison["return_count"].eq(
        returns_comparison["expected_return_count"]
    ).all(),
)

add_check(
    "Orders with returns reconcile",
    returns_comparison["orders_with_returns"].eq(
        returns_comparison["expected_order_count"]
    ).all(),
)

add_check(
    "Returned quantities reconcile",
    returns_comparison["returned_quantity"].eq(
        returns_comparison["expected_quantity"]
    ).all(),
)

add_check(
    "Total return records reconcile",
    int(returns_gold["return_count"].sum()) == len(returns),
)


# ============================================================
# 8. SAVE REPORT AND ENFORCE QUALITY GATE
# ============================================================

passed = sum(checks.values())
total = len(checks)
failed = total - passed

overall_status = "PASS" if failed == 0 else "FAIL"

print("\n========== FINAL GOLD SUMMARY ==========")
print(f"Checks passed: {passed}/{total}")
print(f"Checks failed: {failed}")
print(f"Overall status: {overall_status}")

report = pd.DataFrame([
    {
        "check_name": name,
        "status": "PASS" if result else "FAIL",
    }
    for name, result in checks.items()
])

report_path = REPORTS / "gold_layer_validation.csv"
report_path.parent.mkdir(parents=True, exist_ok=True)

report.to_csv(report_path, index=False)

print(f"Validation report saved to: {report_path}")

if failed > 0:
    print("\n❌ GOLD VALIDATION FAILED.")
    print("PostgreSQL loading must not proceed.")
    sys.exit(1)

print("\n✅ GOLD VALIDATION PASSED.")
print("All Gold datasets are ready for PostgreSQL loading.")

sys.exit(0)

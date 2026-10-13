
from pathlib import Path
import pandas as pd

# ==================================================
# 1. PROJECT PATHS
# ==================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"
REPORT_DIR = PROJECT_ROOT / "reports"

REPORT_DIR.mkdir(parents=True, exist_ok=True)


# ==================================================
# 2. LOAD BRONZE DATA
# ==================================================

orders = pd.read_parquet(BRONZE_DIR / "orders.parquet")
items = pd.read_parquet(BRONZE_DIR / "order_items.parquet")
customers = pd.read_parquet(BRONZE_DIR / "customers.parquet")
deliveries = pd.read_parquet(BRONZE_DIR / "deliveries.parquet")


# ==================================================
# 3. INITIALIZE QUALITY RESULTS
# ==================================================

quality_results = []


def add_result(check_name, table_name, failed_rows):
    """Store the result of one data-quality check."""

    failed_rows = int(failed_rows)

    quality_results.append({
        "check_name": check_name,
        "table_name": table_name,
        "failed_rows": failed_rows,
        "status": "PASS" if failed_rows == 0 else "FAIL",
    })


# ==================================================
# 4. CHECK 1: DUPLICATE ORDER IDS
# ==================================================

duplicate_orders = orders["order_id"].duplicated().sum()

add_result(
    "Duplicate order IDs",
    "orders",
    duplicate_orders
)


# ==================================================
# 5. CHECK 2: INVALID OR MISSING QUANTITIES
# ==================================================

invalid_quantity = (
    items["quantity"].isna()
    | (items["quantity"] <= 0)
)

add_result(
    "Non-positive or missing quantities",
    "order_items",
    invalid_quantity.sum()
)


# ==================================================
# 6. CHECK 3: MISSING CUSTOMER IDS
# ==================================================

missing_customer_ids = orders["customer_id"].isna().sum()

add_result(
    "Missing customer IDs",
    "orders",
    missing_customer_ids
)


# ==================================================
# 7. CHECK 4: UNEXPECTED ORDER STATUSES
# ==================================================

valid_order_statuses = [
    "PLACED",
    "PENDING",
    "CONFIRMED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
]

invalid_order_status = (
    orders["order_status"].isna()
    | ~orders["order_status"].isin(valid_order_statuses)
)

add_result(
    "Unexpected order statuses",
    "orders",
    invalid_order_status.sum()
)


# ==================================================
# 8. CHECK 5: INVALID DISCOUNTS
# ==================================================

invalid_discount = (
    items["discount_pct"].notna()
    & (
        (items["discount_pct"] < 0)
        | (items["discount_pct"] > 100)
    )
)

add_result(
    "Discount outside 0-100",
    "order_items",
    invalid_discount.sum()
)


# ==================================================
# 9. CHECK 6: ORPHAN CUSTOMER REFERENCES
# ==================================================

valid_customer_ids = customers["customer_id"].dropna()

orphan_customers = orders[
    orders["customer_id"].notna()
    & ~orders["customer_id"].isin(valid_customer_ids)
]

add_result(
    "Orphan customer references",
    "orders",
    len(orphan_customers)
)


# ==================================================
# 10. PREPARE DELIVERY DATES
# ==================================================

deliveries["dispatch_date_parsed"] = pd.to_datetime(
    deliveries["dispatch_date"],
    errors="coerce"
)

deliveries["expected_delivery_date_parsed"] = pd.to_datetime(
    deliveries["expected_delivery_date"],
    errors="coerce"
)

deliveries["actual_delivery_date_parsed"] = pd.to_datetime(
    deliveries["actual_delivery_date"],
    errors="coerce"
)


# ==================================================
# 11. CHECK 7: ACTUAL DELIVERY BEFORE DISPATCH
# ==================================================

invalid_delivery_sequence = (
    deliveries["dispatch_date_parsed"].notna()
    & deliveries["actual_delivery_date_parsed"].notna()
    & (
        deliveries["actual_delivery_date_parsed"]
        < deliveries["dispatch_date_parsed"]
    )
)

add_result(
    "Actual delivery before dispatch",
    "deliveries",
    invalid_delivery_sequence.sum()
)


# ==================================================
# 12. CHECK 8: DELIVERED WITHOUT ACTUAL DATE
# ==================================================

missing_actual_for_delivered = (
    (deliveries["delivery_status"] == "DELIVERED")
    & deliveries["actual_delivery_date_parsed"].isna()
)

add_result(
    "Delivered without actual date",
    "deliveries",
    missing_actual_for_delivered.sum()
)


# ==================================================
# 13. GENERATE QUALITY REPORT
# ==================================================

report = pd.DataFrame(quality_results)

print("\n========== DATA QUALITY REPORT ==========\n")
print(report.to_string(index=False))

print("\nTotal checks:", len(report))
print("Passed checks:", (report["status"] == "PASS").sum())
print("Failed checks:", (report["status"] == "FAIL").sum())


# ==================================================
# 14. EXPORT REPORT TO CSV
# ==================================================

report_path = REPORT_DIR / "quality_report.csv"

report.to_csv(report_path, index=False)

print(f"\nQuality report saved to: {report_path}")

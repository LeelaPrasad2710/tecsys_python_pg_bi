
from pathlib import Path
import pandas as pd
import sys

# ============================================================
# 1. PROJECT PATHS
# ============================================================

ROOT = Path(__file__).resolve().parents[2]

BRONZE = ROOT / "data_lake" / "bronze"
SILVER = ROOT / "data_lake" / "silver"
QUARANTINE = ROOT / "data_lake" / "quarantine"
REPORTS = ROOT / "reports"

REPORTS.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. LOAD ALL SILVER TABLES
# ============================================================

table_names = [
    "customers",
    "products",
    "orders",
    "order_items",
    "deliveries",
    "returns",
]

tables = {}

for table_name in table_names:
    silver_path = SILVER / f"{table_name}.parquet"

    if not silver_path.exists():
        print(f"ERROR: Missing Silver file: {silver_path}")
        sys.exit(1)

    tables[table_name] = pd.read_parquet(silver_path)


# ============================================================
# 3. DEFINE PRIMARY KEYS AND FOREIGN KEYS
# ============================================================

primary_keys = {
    "customers": "customer_id",
    "products": "product_id",
    "orders": "order_id",
    "order_items": "order_item_id",
    "deliveries": "delivery_id",
    "returns": "return_id",
}

foreign_keys = [
    ("orders", "customer_id", "customers", "customer_id"),
    ("order_items", "order_id", "orders", "order_id"),
    ("order_items", "product_id", "products", "product_id"),
    ("deliveries", "order_id", "orders", "order_id"),
    ("returns", "order_id", "orders", "order_id"),
]

results = []


# ============================================================
# 4. HELPER FUNCTION TO RECORD RESULTS
# ============================================================

def add_result(check, table, failed_rows, status, details=""):
    results.append({
        "check": check,
        "table": table,
        "failed_rows": int(failed_rows),
        "status": status,
        "details": details,
    })


# ============================================================
# 5. VALIDATE PRIMARY KEYS
# ============================================================

print("\n========== PRIMARY KEY CHECKS ==========")

for table_name, key in primary_keys.items():
    df = tables[table_name]

    if key not in df.columns:
        print(f"ERROR: Column {key} missing from {table_name}")
        sys.exit(1)

    missing = int(df[key].isna().sum())
    duplicates = int(df[key].dropna().duplicated().sum())

    failures = missing + duplicates
    status = "PASS" if failures == 0 else "FAIL"

    add_result(
        "PRIMARY_KEY",
        table_name,
        failures,
        status,
        f"Missing={missing}; duplicate occurrences={duplicates}",
    )

    print(
        f"{table_name}: missing={missing}, "
        f"duplicate occurrences={duplicates} -> {status}"
    )


# ============================================================
# 6. VALIDATE FOREIGN KEYS
# ============================================================

print("\n========== FOREIGN KEY CHECKS ==========")

for child, child_key, parent, parent_key in foreign_keys:
    child_df = tables[child]
    parent_df = tables[parent]

    if child_key not in child_df.columns:
        print(f"ERROR: Column {child_key} missing from {child}")
        sys.exit(1)

    if parent_key not in parent_df.columns:
        print(f"ERROR: Column {parent_key} missing from {parent}")
        sys.exit(1)

    parent_ids = set(parent_df[parent_key].dropna())

    missing = child_df[child_key].isna()

    orphan = (
        child_df[child_key].notna()
        & ~child_df[child_key].isin(parent_ids)
    )

    missing_count = int(missing.sum())
    orphan_count = int(orphan.sum())

    failures = missing_count + orphan_count
    status = "PASS" if failures == 0 else "FAIL"

    add_result(
        f"FOREIGN_KEY_{child_key}",
        child,
        failures,
        status,
        f"References {parent}.{parent_key}; "
        f"missing={missing_count}; orphan={orphan_count}",
    )

    print(
        f"{child}.{child_key} -> {parent}.{parent_key}: "
        f"missing={missing_count}, orphan={orphan_count} "
        f"-> {status}"
    )


# ============================================================
# 7. RECONCILE BRONZE, SILVER AND QUARANTINE ROW COUNTS
# ============================================================

print("\n========== ROW RECONCILIATION ==========")

for table_name in primary_keys:
    bronze_path = BRONZE / f"{table_name}.parquet"
    silver_path = SILVER / f"{table_name}.parquet"
    quarantine_path = QUARANTINE / f"{table_name}_issues.parquet"

    # Verify required files exist.
    if not bronze_path.exists():
        print(f"ERROR: Missing Bronze file: {bronze_path}")
        sys.exit(1)

    if not silver_path.exists():
        print(f"ERROR: Missing Silver file: {silver_path}")
        sys.exit(1)

    bronze_df = pd.read_parquet(bronze_path)
    silver_df = pd.read_parquet(silver_path)

    bronze_count = len(bronze_df)
    silver_count = len(silver_df)

    quarantine_count = (
        len(pd.read_parquet(quarantine_path))
        if quarantine_path.exists()
        else 0
    )

    # Customers and products remove exact duplicate rows
    # without putting those duplicate copies into quarantine.
    dedup_removed = 0

    if table_name in {"customers", "products"}:
        dedup_removed = int(bronze_df.duplicated().sum())

    accounted = (
        silver_count
        + quarantine_count
        + dedup_removed
    )

    difference = bronze_count - accounted

    status = "PASS" if difference == 0 else "FAIL"

    add_result(
        "ROW_RECONCILIATION",
        table_name,
        abs(difference),
        status,
        f"Bronze={bronze_count}; "
        f"Silver={silver_count}; "
        f"Quarantine={quarantine_count}; "
        f"exact duplicates removed={dedup_removed}; "
        f"difference={difference}",
    )

    print(
        f"{table_name}: Bronze={bronze_count}, "
        f"Silver={silver_count}, "
        f"Quarantine={quarantine_count}, "
        f"Deduplicated={dedup_removed}, "
        f"Difference={difference} -> {status}"
    )


# ============================================================
# 8. SAVE CONSOLIDATED REPORT
# ============================================================

report = pd.DataFrame(results)

report_path = REPORTS / "cross_table_reconciliation.csv"

report.to_csv(report_path, index=False)

print("\n========== FINAL SUMMARY ==========")
print(report["status"].value_counts().to_string())

print(f"\nReport saved to: {report_path}")


# ============================================================
# 9. QUALITY GATE
# Exit with code 1 if ANY check fails.
# Exit with code 0 only if ALL checks pass.
# ============================================================

failed_checks = report[report["status"] != "PASS"]

if not failed_checks.empty:
    print("\n❌ RECONCILIATION FAILED")
    print(f"Failed checks: {len(failed_checks)}")

    print(
        failed_checks[
            ["check", "table", "failed_rows", "details"]
        ].to_string(index=False)
    )

    sys.exit(1)

print("\n✅ RECONCILIATION PASSED: All checks passed.")
sys.exit(0)

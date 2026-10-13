
from pathlib import Path
import pandas as pd

# ============================================================
# 1. PROJECT PATHS
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data_lake" / "silver"
QUARANTINE_DIR = PROJECT_ROOT / "data_lake" / "quarantine"

SILVER_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# 2. LOAD DATA
# ============================================================

orders = pd.read_parquet(BRONZE_DIR / "orders.parquet")
customers = pd.read_parquet(SILVER_DIR / "customers.parquet")

print("Bronze orders:", len(orders))

# ============================================================
# 3. DEFINE VALIDATION RULES
# ============================================================

# Missing order IDs
missing_order_id = orders["order_id"].isna()

# Repeated order IDs after the first occurrence.
# Exact duplicate records will be handled separately below.
duplicate_order_id = (
    orders["order_id"].notna()
    & orders["order_id"].duplicated(keep="first")
)

# Missing customer IDs
missing_customer_id = orders["customer_id"].isna()

# Customer IDs that don't exist in Silver customers
orphan_customer_reference = (
    orders["customer_id"].notna()
    & ~orders["customer_id"].isin(customers["customer_id"])
)

# Allowed order statuses
valid_statuses = [
    "PLACED",
    "PENDING",
    "CONFIRMED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
]

invalid_status = (
    orders["order_status"].isna()
    | ~orders["order_status"].isin(valid_statuses)
)

# Validate order dates
parsed_order_date = pd.to_datetime(
    orders["order_date"],
    errors="coerce"
)

invalid_order_date = parsed_order_date.isna()

# ============================================================
# 4. IDENTIFY CONFLICTING DUPLICATE ORDER IDS
# ============================================================

duplicate_groups = orders.loc[
    orders["order_id"].notna()
    & orders["order_id"].duplicated(keep=False)
]

conflicting_order_ids = []

for order_id, group in duplicate_groups.groupby("order_id"):
    if len(group.drop_duplicates()) > 1:
        conflicting_order_ids.append(order_id)

conflicting_duplicate = orders["order_id"].isin(
    conflicting_order_ids
)

# Quarantine every row for a conflicting order ID.
# For exact duplicate records, keep the first and quarantine repeats.
exact_duplicate_repeat = (
    duplicate_order_id & ~conflicting_duplicate
)

# ============================================================
# 5. COMBINE ALL FAILURE RULES
# ============================================================

rules = {
    "MISSING_ORDER_ID": missing_order_id,
    "DUPLICATE_ORDER_ID": (
        conflicting_duplicate | exact_duplicate_repeat
    ),
    "MISSING_CUSTOMER_ID": missing_customer_id,
    "ORPHAN_CUSTOMER_REFERENCE": orphan_customer_reference,
    "INVALID_ORDER_STATUS": invalid_status,
    "INVALID_ORDER_DATE": invalid_order_date,
}

quarantine_mask = pd.Series(False, index=orders.index)

for rule_mask in rules.values():
    quarantine_mask |= rule_mask

# ============================================================
# 6. CREATE QUARANTINE WITH FAILURE REASONS
# ============================================================

quarantine = orders.loc[quarantine_mask].copy()

quarantine["failure_reason"] = ""

for reason, rule_mask in rules.items():
    affected_index = orders.index[rule_mask].intersection(
        quarantine.index
    )

    existing = quarantine.loc[
        affected_index, "failure_reason"
    ]

    quarantine.loc[affected_index, "failure_reason"] = (
        existing.where(existing.eq(""), existing + ";")
        + reason
    )

# ============================================================
# 7. BUILD SILVER ORDERS
# ============================================================

silver_orders = orders.loc[~quarantine_mask].copy()

# Use parsed dates consistently in Silver.
silver_orders["order_date"] = parsed_order_date.loc[
    silver_orders.index
]

# ============================================================
# 8. SAVE OUTPUTS
# ============================================================

silver_path = SILVER_DIR / "orders.parquet"
quarantine_path = (
    QUARANTINE_DIR / "orders_issues.parquet"
)

silver_orders.to_parquet(silver_path, index=False)
quarantine.to_parquet(quarantine_path, index=False)

# ============================================================
# 9. REPORT RESULTS
# ============================================================

print("\n========== SILVER ORDERS REPORT ==========")
print("Bronze orders:", len(orders))
print("Silver orders:", len(silver_orders))
print("Quarantined rows:", len(quarantine))
print("Conflicting duplicate order IDs:", len(conflicting_order_ids))

print("\nCounts by validation rule:")
for reason, rule_mask in rules.items():
    print(f"{reason}: {int(rule_mask.sum())}")

print("\nQuarantine reason combinations:")
if quarantine.empty:
    print("No quarantined orders.")
else:
    print(quarantine["failure_reason"].value_counts().to_string())

print("\nSilver saved to:", silver_path)
print("Quarantine saved to:", quarantine_path)
print("\nSilver orders build completed.")

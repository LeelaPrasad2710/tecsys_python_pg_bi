
from pathlib import Path
import pandas as pd

# 1. PROJECT PATHS
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"
QUARANTINE_DIR = PROJECT_ROOT / "data_lake" / "quarantine"
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

# 2. LOAD BRONZE DATA
orders = pd.read_parquet(BRONZE_DIR / "orders.parquet")
customers = pd.read_parquet(BRONZE_DIR / "customers.parquet")

print("Total Bronze orders:", len(orders))

# 3. DEFINE VALIDATION RULES
valid_customer_ids = customers["customer_id"].dropna()

valid_order_statuses = [
    "PLACED",
    "PENDING",
    "CONFIRMED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
]

# True for repeated occurrences after the first
duplicate_mask = orders["order_id"].duplicated()

missing_customer_mask = orders["customer_id"].isna()

orphan_customer_mask = (
    orders["customer_id"].notna()
    & ~orders["customer_id"].isin(valid_customer_ids)
)

invalid_status_mask = (
    orders["order_status"].isna()
    | ~orders["order_status"].isin(valid_order_statuses)
)

# 4. COMBINE ALL VALIDATION RULES
quarantine_mask = (
    duplicate_mask
    | missing_customer_mask
    | orphan_customer_mask
    | invalid_status_mask
)

quarantine = orders.loc[quarantine_mask].copy()

# 5. ATTACH FAILURE REASONS
quarantine["failure_reason"] = ""

def add_reason(mask, reason):
    affected_index = orders.index[mask].intersection(quarantine.index)

    existing_reasons = quarantine.loc[
        affected_index, "failure_reason"
    ]

    quarantine.loc[affected_index, "failure_reason"] = (
        existing_reasons
        .where(existing_reasons.eq(""), existing_reasons + ";")
        + reason
    )

add_reason(duplicate_mask, "DUPLICATE_ORDER_ID")
add_reason(missing_customer_mask, "MISSING_CUSTOMER_ID")
add_reason(orphan_customer_mask, "ORPHAN_CUSTOMER_REFERENCE")
add_reason(invalid_status_mask, "INVALID_ORDER_STATUS")

# 6. DISPLAY RESULTS
print("Orders passing these checks:", int((~quarantine_mask).sum()))
print("Orders quarantined:", len(quarantine))

print("\nQuarantine reasons:")
print(quarantine["failure_reason"].value_counts())

print("\nQuarantined records:")
print(quarantine.to_string(index=False))

# 7. SAVE OUTPUTS
parquet_path = QUARANTINE_DIR / "orders_issues.parquet"
csv_path = QUARANTINE_DIR / "orders_issues.csv"

quarantine.to_parquet(parquet_path, index=False)
quarantine.to_csv(csv_path, index=False)

print("\nSaved quarantine files:")
print(parquet_path)
print(csv_path)
print("\nOrder quarantine completed.")

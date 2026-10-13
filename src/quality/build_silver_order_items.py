
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
# 2. LOAD SOURCE AND REFERENCE TABLES
# ============================================================

items = pd.read_parquet(BRONZE_DIR / "order_items.parquet")
orders = pd.read_parquet(SILVER_DIR / "orders.parquet")
products = pd.read_parquet(SILVER_DIR / "products.parquet")

print("Bronze order items:", len(items))


# ============================================================
# 3. DEFINE VALIDATION RULES
# ============================================================

# Item primary key
missing_item_id = items["order_item_id"].isna()

duplicate_item_id = (
    items["order_item_id"].notna()
    & items["order_item_id"].duplicated(keep="first")
)

# Order foreign key: validate against Silver orders
missing_order_id = items["order_id"].isna()

orphan_order_reference = (
    items["order_id"].notna()
    & ~items["order_id"].isin(orders["order_id"])
)

# Product foreign key: validate against Silver products
missing_product_id = items["product_id"].isna()

orphan_product_reference = (
    items["product_id"].notna()
    & ~items["product_id"].isin(products["product_id"])
)

# Quantity must be positive
invalid_quantity = (
    items["quantity"].isna()
    | (items["quantity"] <= 0)
)

# Unit price must be present and non-negative
invalid_unit_price = (
    items["unit_price"].isna()
    | (items["unit_price"] < 0)
)

# Discount, when supplied, must be between 0 and 100
invalid_discount = (
    items["discount_pct"].notna()
    & (
        (items["discount_pct"] < 0)
        | (items["discount_pct"] > 100)
    )
)


# ============================================================
# 4. COMBINE VALIDATION RULES
# ============================================================

rules = {
    "MISSING_ORDER_ITEM_ID": missing_item_id,
    "DUPLICATE_ORDER_ITEM_ID": duplicate_item_id,
    "MISSING_ORDER_ID": missing_order_id,
    "ORPHAN_ORDER_REFERENCE": orphan_order_reference,
    "MISSING_PRODUCT_ID": missing_product_id,
    "ORPHAN_PRODUCT_REFERENCE": orphan_product_reference,
    "INVALID_QUANTITY": invalid_quantity,
    "INVALID_UNIT_PRICE": invalid_unit_price,
    "INVALID_DISCOUNT": invalid_discount,
}

quarantine_mask = pd.Series(False, index=items.index)

for rule_mask in rules.values():
    quarantine_mask |= rule_mask


# ============================================================
# 5. CREATE QUARANTINE WITH FAILURE REASONS
# ============================================================

quarantine = items.loc[quarantine_mask].copy()
quarantine["failure_reason"] = ""

for reason, rule_mask in rules.items():
    affected_index = items.index[rule_mask].intersection(
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
# 6. BUILD SILVER ORDER ITEMS
# ============================================================

silver_items = items.loc[~quarantine_mask].copy()


# ============================================================
# 7. SAVE OUTPUTS
# ============================================================

silver_path = SILVER_DIR / "order_items.parquet"

quarantine_path = (
    QUARANTINE_DIR / "order_items_issues.parquet"
)

silver_items.to_parquet(silver_path, index=False)
quarantine.to_parquet(quarantine_path, index=False)


# ============================================================
# 8. REPORT RESULTS
# ============================================================

print("\n========== SILVER ORDER ITEMS REPORT ==========")

print("Bronze rows:", len(items))
print("Silver rows:", len(silver_items))
print("Quarantined rows:", len(quarantine))

print("\nCounts by validation rule:")

for reason, rule_mask in rules.items():
    print(f"{reason}: {int(rule_mask.sum())}")

print("\nQuarantine reason combinations:")

if quarantine.empty:
    print("No quarantined items.")
else:
    print(quarantine["failure_reason"].value_counts().to_string())

print("\nSilver saved to:", silver_path)
print("Quarantine saved to:", quarantine_path)

print("\nSilver order-items build completed.")

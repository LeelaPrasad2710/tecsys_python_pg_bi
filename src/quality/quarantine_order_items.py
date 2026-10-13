
from pathlib import Path
import pandas as pd

# 1. PROJECT PATHS
PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"
QUARANTINE_DIR = PROJECT_ROOT / "data_lake" / "quarantine"
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

# 2. LOAD BRONZE TABLES
items = pd.read_parquet(BRONZE_DIR / "order_items.parquet")
orders = pd.read_parquet(
    PROJECT_ROOT / "data_lake" / "silver" / "orders.parquet"
)
products = pd.read_parquet(
    PROJECT_ROOT / "data_lake" / "silver" / "products.parquet"
)

print("Total Bronze order items:", len(items))

# 3. BUILD REFERENCE ID LISTS
valid_order_ids = orders["order_id"].dropna()
valid_product_ids = products["product_id"].dropna()

# 4. DEFINE VALIDATION RULES
missing_item_id = items["order_item_id"].isna()

duplicate_item_id = (
    items["order_item_id"].notna()
    & items["order_item_id"].duplicated(keep="first")
)

missing_order_id = items["order_id"].isna()
orphan_order_id = (
    items["order_id"].notna()
    & ~items["order_id"].isin(valid_order_ids)
)

missing_product_id = items["product_id"].isna()
orphan_product_id = (
    items["product_id"].notna()
    & ~items["product_id"].isin(valid_product_ids)
)

invalid_quantity = items["quantity"].isna() | (items["quantity"] <= 0)

invalid_unit_price = (
    items["unit_price"].isna()
    | (items["unit_price"] < 0)
)

invalid_discount = (
    items["discount_pct"].notna()
    & (
        (items["discount_pct"] < 0)
        | (items["discount_pct"] > 100)
    )
)

# 5. COMBINE THE RULES
rules = {
    "MISSING_ORDER_ITEM_ID": missing_item_id,
    "DUPLICATE_ORDER_ITEM_ID": duplicate_item_id,
    "MISSING_ORDER_ID": missing_order_id,
    "ORPHAN_ORDER_REFERENCE": orphan_order_id,
    "MISSING_PRODUCT_ID": missing_product_id,
    "ORPHAN_PRODUCT_REFERENCE": orphan_product_id,
    "INVALID_QUANTITY": invalid_quantity,
    "INVALID_UNIT_PRICE": invalid_unit_price,
    "INVALID_DISCOUNT": invalid_discount,
}

quarantine_mask = pd.Series(False, index=items.index)

for rule_mask in rules.values():
    quarantine_mask |= rule_mask

quarantine = items.loc[quarantine_mask].copy()

# 6. ATTACH ALL FAILURE REASONS
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

# 7. REPORT RESULTS
print("Rows passing these checks:", int((~quarantine_mask).sum()))
print("Rows quarantined:", len(quarantine))

print("\nCounts by validation rule:")
for reason, rule_mask in rules.items():
    print(f"{reason}: {int(rule_mask.sum())}")

print("\nQuarantined rows with reasons:")
print(quarantine.to_string(index=False))

# 8. SAVE QUARANTINE OUTPUTS
parquet_path = QUARANTINE_DIR / "order_items_issues.parquet"
csv_path = QUARANTINE_DIR / "order_items_issues.csv"

quarantine.to_parquet(parquet_path, index=False)
quarantine.to_csv(csv_path, index=False)

print("\nSaved quarantine files:")
print(parquet_path)
print(csv_path)
print("\nOrder-items quarantine completed.")


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
# 2. BUILD SILVER CUSTOMERS
# ============================================================

def build_customers():

    customers = pd.read_parquet(
        BRONZE_DIR / "customers.parquet"
    )

    print("\n========== CUSTOMERS ==========")
    print("Bronze rows:", len(customers))

    # Rule 1: Missing customer IDs cannot identify customers.
    missing_key = customers["customer_id"].isna()

    # Rule 2: Find duplicate customer IDs.
    valid = customers.loc[~missing_key].copy()

    duplicate_groups = valid.loc[
        valid["customer_id"].duplicated(keep=False)
    ]

    # Identify duplicate IDs with conflicting records.
    conflicting_ids = []

    for customer_id, group in duplicate_groups.groupby(
        "customer_id"
    ):
        if len(group.drop_duplicates()) > 1:
            conflicting_ids.append(customer_id)

    conflict_mask = valid["customer_id"].isin(
        conflicting_ids
    )

    # Collect records that cannot safely enter Silver.
    quarantine = pd.concat(
        [
            customers.loc[missing_key].copy(),
            valid.loc[conflict_mask].copy(),
        ],
        ignore_index=True,
    )

    # Add explicit failure reasons.
    quarantine["failure_reason"] = ""

    quarantine.loc[
        quarantine["customer_id"].isna(),
        "failure_reason",
    ] = "MISSING_CUSTOMER_ID"

    quarantine.loc[
        quarantine["customer_id"].isin(conflicting_ids),
        "failure_reason",
    ] = "CONFLICTING_CUSTOMER_ID"

    # Keep records that passed the checks.
    accepted = valid.loc[~conflict_mask].copy()

    # Remove exact duplicate customer records by primary key.
    silver = accepted.drop_duplicates(
        subset=["customer_id"],
        keep="first",
    )

    # Save outputs.
    silver_path = SILVER_DIR / "customers.parquet"
    quarantine_path = (
        QUARANTINE_DIR / "customers_issues.parquet"
    )

    silver.to_parquet(silver_path, index=False)
    quarantine.to_parquet(quarantine_path, index=False)

    print("Silver rows:", len(silver))
    print("Exact duplicate rows removed:", len(accepted) - len(silver))
    print("Conflicting customer IDs:", len(conflicting_ids))
    print("Quarantined rows:", len(quarantine))
    print("Silver saved to:", silver_path)
    print("Quarantine saved to:", quarantine_path)


# ============================================================
# 3. BUILD SILVER PRODUCTS
# ============================================================

def build_products():

    products = pd.read_parquet(
        BRONZE_DIR / "products.parquet"
    )

    print("\n========== PRODUCTS ==========")
    print("Bronze rows:", len(products))

    # Rule 1: Missing product IDs.
    missing_key = products["product_id"].isna()

    valid = products.loc[~missing_key].copy()

    # Rule 2: Identify duplicate product IDs.
    duplicate_groups = valid.loc[
        valid["product_id"].duplicated(keep=False)
    ]

    conflicting_ids = []

    for product_id, group in duplicate_groups.groupby(
        "product_id"
    ):
        if len(group.drop_duplicates()) > 1:
            conflicting_ids.append(product_id)

    conflict_mask = valid["product_id"].isin(
        conflicting_ids
    )

    # Rule 3: Negative or missing product prices.
    invalid_price = (
        valid["unit_price"].isna()
        | (valid["unit_price"] < 0)
    )

    # Combine the rules to identify rejected rows.
    quarantine_mask = conflict_mask | invalid_price

    quarantine = pd.concat(
        [
            products.loc[missing_key].copy(),
            valid.loc[quarantine_mask].copy(),
        ],
        ignore_index=True,
    )

    # Attach all applicable failure reasons.
    quarantine["failure_reason"] = ""

    def add_product_reason(mask, reason):
        affected_index = quarantine.index[
            quarantine["product_id"].isin(
                valid.loc[mask, "product_id"]
            )
        ]

        existing = quarantine.loc[
            affected_index, "failure_reason"
        ]

        quarantine.loc[affected_index, "failure_reason"] = (
            existing.where(existing.eq(""), existing + ";")
            + reason
        )

    # Missing primary keys.
    quarantine.loc[
        quarantine["product_id"].isna(),
        "failure_reason",
    ] = "MISSING_PRODUCT_ID"

    # Conflicting duplicate keys.
    add_product_reason(
        conflict_mask,
        "CONFLICTING_PRODUCT_ID",
    )

    # Missing or negative prices.
    add_product_reason(
        invalid_price,
        "INVALID_UNIT_PRICE",
    )

    # Keep accepted records and deduplicate exact copies.
    accepted = valid.loc[~quarantine_mask].copy()

    silver = accepted.drop_duplicates(
        subset=["product_id"],
        keep="first",
    )

    # Rule 4: Preserve products with missing categories.
    missing_category = silver["category"].isna()

    silver.loc[
        missing_category, "category"
    ] = "UNKNOWN"

    # Explicitly flag categories replaced by UNKNOWN.
    silver["category_was_missing"] = missing_category

    # Save outputs.
    silver_path = SILVER_DIR / "products.parquet"
    quarantine_path = (
        QUARANTINE_DIR / "products_issues.parquet"
    )

    silver.to_parquet(silver_path, index=False)
    quarantine.to_parquet(quarantine_path, index=False)

    print("Silver rows:", len(silver))
    print("Exact duplicate rows removed:", len(accepted) - len(silver))
    print("Conflicting product IDs:", len(conflicting_ids))
    print("Invalid-price rows:", int(invalid_price.sum()))
    print("Quarantined rows:", len(quarantine))
    print(
        "Missing categories replaced with UNKNOWN:",
        int(missing_category.sum()),
    )

    print("\nQuarantine reasons:")
    if quarantine.empty:
        print("No rejected products.")
    else:
        print(
            quarantine["failure_reason"]
            .value_counts()
            .to_string()
        )

    print("Silver saved to:", silver_path)
    print("Quarantine saved to:", quarantine_path)


# ============================================================
# 4. RUN THE DIMENSION PIPELINE
# ============================================================

if __name__ == "__main__":
    build_customers()
    build_products()

    print("\nSilver dimension build completed.")

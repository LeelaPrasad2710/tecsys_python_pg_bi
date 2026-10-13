
from pathlib import Path
import pandas as pd

# --------------------------------------------------
# 1. Project paths
# --------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data_lake" / "silver"
QUARANTINE_DIR = PROJECT_ROOT / "data_lake" / "quarantine"

SILVER_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# 2. Load Bronze returns and Silver orders
# --------------------------------------------------
df = pd.read_parquet(BRONZE_DIR / "returns.parquet").copy()
orders = pd.read_parquet(SILVER_DIR / "orders.parquet")

print(f"Bronze returns: {len(df)}")


# --------------------------------------------------
# 3. Normalize values
# --------------------------------------------------
for col in ["return_id", "order_id", "return_reason"]:
    df[col] = df[col].astype("string").str.strip()

df["return_date"] = pd.to_datetime(
    df["return_date"], errors="coerce"
)

df["return_quantity"] = pd.to_numeric(
    df["return_quantity"], errors="coerce"
)

valid_order_ids = set(orders["order_id"].dropna())


# --------------------------------------------------
# 4. Define approved return reasons
# --------------------------------------------------
VALID_RETURN_REASONS = {
    "SIZE_ISSUE",
    "DAMAGED",
    "WRONG_ITEM",
    "QUALITY_ISSUE",
    "CUSTOMER_CHANGED_MIND",
}


# --------------------------------------------------
# 5. Validate duplicate return IDs
# --------------------------------------------------
missing_return_id = (
    df["return_id"].isna() | df["return_id"].eq("")
)

duplicate_return_id = pd.Series(False, index=df.index)
conflicting_return_id = pd.Series(False, index=df.index)

duplicate_ids = df.loc[
    ~missing_return_id
    & df["return_id"].duplicated(keep=False),
    "return_id",
].unique()

# Exact duplicate records: keep the first, quarantine repeats.
# Conflicting records with the same ID: quarantine all copies.
for return_id in duplicate_ids:
    group = df.loc[df["return_id"].eq(return_id)]
    distinct_rows = group.drop_duplicates()

    if len(distinct_rows) > 1:
        conflicting_return_id.loc[group.index] = True
    else:
        duplicate_return_id.loc[group.index[1:]] = True


# --------------------------------------------------
# 6. Define validation rules
# --------------------------------------------------
missing_reason = (
    df["return_reason"].isna()
    | df["return_reason"].eq("")
)

invalid_reason = (
    ~missing_reason
    & ~df["return_reason"].isin(VALID_RETURN_REASONS)
)

rules = {
    "MISSING_RETURN_ID": missing_return_id,
    "DUPLICATE_RETURN_ID": duplicate_return_id,
    "CONFLICTING_RETURN_ID": conflicting_return_id,

    "MISSING_ORDER_ID": (
        df["order_id"].isna() | df["order_id"].eq("")
    ),

    "ORPHAN_ORDER_REFERENCE": (
        df["order_id"].notna()
        & df["order_id"].ne("")
        & ~df["order_id"].isin(valid_order_ids)
    ),

    "INVALID_RETURN_DATE": df["return_date"].isna(),

    "MISSING_RETURN_REASON": missing_reason,
    "INVALID_RETURN_REASON": invalid_reason,

    "INVALID_RETURN_QUANTITY": (
        df["return_quantity"].isna()
        | (df["return_quantity"] <= 0)
    ),
}


# --------------------------------------------------
# 7. Build combined quarantine reasons
# --------------------------------------------------
reasons = pd.Series("", index=df.index, dtype="string")

for reason, mask in rules.items():
    reasons.loc[mask] = reasons.loc[mask].apply(
        lambda existing: (
            f"{existing};{reason}" if existing else reason
        )
    )

invalid_rows = reasons.ne("")

quarantine = df.loc[invalid_rows].copy()
quarantine["failure_reason"] = reasons.loc[invalid_rows]

silver = df.loc[~invalid_rows].copy()


# --------------------------------------------------
# 8. Save Silver and quarantine outputs
# --------------------------------------------------
silver_path = SILVER_DIR / "returns.parquet"
quarantine_path = QUARANTINE_DIR / "returns_issues.parquet"

silver.to_parquet(silver_path, index=False)
quarantine.to_parquet(quarantine_path, index=False)

quarantine.to_csv(
    QUARANTINE_DIR / "returns_issues.csv",
    index=False,
)


# --------------------------------------------------
# 9. Print validation report
# --------------------------------------------------
print("\n========== SILVER RETURNS REPORT ==========")
print(f"Bronze rows: {len(df)}")
print(f"Silver rows: {len(silver)}")
print(f"Quarantined rows: {len(quarantine)}")

print("\nCounts by validation rule:")
for reason, mask in rules.items():
    print(f"{reason}: {int(mask.sum())}")

print("\nQuarantine reason combinations:")
if quarantine.empty:
    print("No quarantined records.")
else:
    print(
        quarantine["failure_reason"]
        .value_counts()
        .to_string()
    )

print("\nInvalid return reasons found:")
if invalid_reason.any():
    print(
        df.loc[invalid_reason, "return_reason"]
        .value_counts()
        .to_string()
    )
else:
    print("None")

print(f"\nSilver saved to: {silver_path}")
print(f"Quarantine saved to: {quarantine_path}")

print("\nSilver returns build completed.")

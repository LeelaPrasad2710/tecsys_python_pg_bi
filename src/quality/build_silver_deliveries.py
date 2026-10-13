from pathlib import Path
import pandas as pd

# 1. Project paths
PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"
SILVER_DIR = PROJECT_ROOT / "data_lake" / "silver"
QUARANTINE_DIR = PROJECT_ROOT / "data_lake" / "quarantine"

SILVER_DIR.mkdir(parents=True, exist_ok=True)
QUARANTINE_DIR.mkdir(parents=True, exist_ok=True)

# 2. Load Bronze deliveries and Silver orders
deliveries = pd.read_parquet(BRONZE_DIR / "deliveries.parquet")
orders = pd.read_parquet(SILVER_DIR / "orders.parquet")

df = deliveries.copy()

# 3. Normalize values and dates
df["delivery_status"] = (
    df["delivery_status"].astype("string").str.strip().str.upper()
)

for col in [
    "dispatch_date",
    "expected_delivery_date",
    "actual_delivery_date",
]:
    df[col] = pd.to_datetime(df[col], errors="coerce")

allowed_statuses = {
    "DELIVERED",
    "SHIPPED",
    "PROCESSING",
    "CANCELLED",
    "FAILED",
}

valid_order_ids = set(orders["order_id"].dropna())

# 4. Define validation rules
rules = {
    "MISSING_DELIVERY_ID": (
        df["delivery_id"].isna()
        | df["delivery_id"].astype("string").str.strip().eq("")
    ),

    "DUPLICATE_DELIVERY_ID": (
        df["delivery_id"].notna()
        & df["delivery_id"].duplicated(keep="first")
    ),

    "MISSING_ORDER_ID": (
        df["order_id"].isna()
        | df["order_id"].astype("string").str.strip().eq("")
    ),

    "ORPHAN_ORDER_REFERENCE": (
        df["order_id"].notna()
        & ~df["order_id"].isin(valid_order_ids)
    ),

    "INVALID_DISPATCH_DATE": df["dispatch_date"].isna(),

    "INVALID_EXPECTED_DELIVERY_DATE": (
        df["expected_delivery_date"].isna()
    ),

    "INVALID_DELIVERY_STATUS": (
        df["delivery_status"].isna()
        | ~df["delivery_status"].isin(allowed_statuses)
    ),

    "DELIVERED_WITHOUT_ACTUAL_DATE": (
        df["delivery_status"].eq("DELIVERED")
        & df["actual_delivery_date"].isna()
    ),

    "ACTUAL_DELIVERY_BEFORE_DISPATCH": (
        df["actual_delivery_date"].notna()
        & df["dispatch_date"].notna()
        & (
            df["actual_delivery_date"]
            < df["dispatch_date"]
        )
    ),
}

# 5. Combine the rules
failure_reason = pd.Series("", index=df.index, dtype="string")

for reason, mask in rules.items():
    failure_reason = failure_reason.mask(
        mask,
        failure_reason.where(
            failure_reason.eq(""),
            failure_reason + ";",
        ) + reason,
    )

invalid_rows = failure_reason.ne("")

quarantine = df.loc[invalid_rows].copy()
quarantine["failure_reason"] = failure_reason.loc[invalid_rows]

silver = df.loc[~invalid_rows].copy()

# 6. Save Silver and quarantine outputs
silver_path = SILVER_DIR / "deliveries.parquet"
quarantine_path = QUARANTINE_DIR / "deliveries_issues.parquet"

silver.to_parquet(silver_path, index=False)
quarantine.to_parquet(quarantine_path, index=False)

quarantine.to_csv(
    QUARANTINE_DIR / "deliveries_issues.csv",
    index=False,
)

# 7. Print the report
print("\n========== SILVER DELIVERIES REPORT ==========")
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

print(f"\nSilver saved to: {silver_path}")
print(f"Quarantine saved to: {quarantine_path}")
print("\nSilver deliveries build completed.")

from pathlib import Path
import pandas as pd

# ==================================================
# 1. PROJECT PATHS
# ==================================================
ROOT = Path(__file__).resolve().parents[2]

SILVER = ROOT / "data_lake" / "silver"
GOLD = ROOT / "data_lake" / "gold"

GOLD.mkdir(parents=True, exist_ok=True)

# ==================================================
# 2. LOAD SILVER DATA
# ==================================================
deliveries = pd.read_parquet(
    SILVER / "deliveries.parquet"
)

orders = pd.read_parquet(
    SILVER / "orders.parquet"
)

# ==================================================
# 3. NORMALIZE DATES
# ==================================================
date_columns = [
    "dispatch_date",
    "expected_delivery_date",
    "actual_delivery_date",
]

for column in date_columns:
    deliveries[column] = pd.to_datetime(
        deliveries[column],
        errors="coerce",
    )

# ==================================================
# 4. SELECT COMPLETED DELIVERIES
# ==================================================
completed = deliveries.loc[
    deliveries["delivery_status"].eq("DELIVERED")
    & deliveries["actual_delivery_date"].notna()
    & deliveries["expected_delivery_date"].notna()
    & deliveries["dispatch_date"].notna()
].copy()

# Exclude impossible delivery sequences
completed = completed.loc[
    completed["actual_delivery_date"]
    >= completed["dispatch_date"]
].copy()

# ==================================================
# 5. CALCULATE DELIVERY METRICS
# ==================================================
completed["delivery_duration_days"] = (
    completed["actual_delivery_date"]
    - completed["dispatch_date"]
).dt.days

completed["is_on_time"] = (
    completed["actual_delivery_date"]
    <= completed["expected_delivery_date"]
)

completed["is_late"] = (
    completed["actual_delivery_date"]
    > completed["expected_delivery_date"]
)

# ==================================================
# 6. BUILD OVERALL KPI DATASET
# ==================================================
eligible_count = len(completed)

on_time_count = int(completed["is_on_time"].sum())
late_count = int(completed["is_late"].sum())

on_time_rate = (
    on_time_count / eligible_count * 100
    if eligible_count
    else 0.0
)

average_duration = (
    completed["delivery_duration_days"].mean()
    if eligible_count
    else 0.0
)

gold_delivery = pd.DataFrame([{
    "eligible_deliveries": eligible_count,
    "on_time_deliveries": on_time_count,
    "late_deliveries": late_count,
    "on_time_delivery_pct": round(on_time_rate, 2),
    "average_delivery_duration_days": round(
        average_duration, 2
    ),
}])

# ==================================================
# 7. SAVE GOLD DATASET
# ==================================================
output_path = GOLD / "gold_delivery_performance.parquet"

gold_delivery.to_parquet(output_path, index=False)

# ==================================================
# 8. PRINT REPORT
# ==================================================
print("\n========== GOLD DELIVERY PERFORMANCE ==========")
print(f"Eligible completed deliveries: {eligible_count}")
print(f"On-time deliveries: {on_time_count}")
print(f"Late deliveries: {late_count}")
print(f"On-time delivery rate: {on_time_rate:.2f}%")
print(f"Average delivery duration: {average_duration:.2f} days")

print("\nGold KPI dataset:")
print(gold_delivery.to_string(index=False))

print(f"\nGold saved to: {output_path}")
print("\nGold delivery performance build completed.")

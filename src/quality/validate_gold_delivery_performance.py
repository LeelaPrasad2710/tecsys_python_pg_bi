
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]

SILVER = ROOT / "data_lake" / "silver"
GOLD = ROOT / "data_lake" / "gold"

deliveries = pd.read_parquet(
    SILVER / "deliveries.parquet"
)

gold = pd.read_parquet(
    GOLD / "gold_delivery_performance.parquet"
)

# Independently rebuild the eligible population
for column in [
    "dispatch_date",
    "expected_delivery_date",
    "actual_delivery_date",
]:
    deliveries[column] = pd.to_datetime(
        deliveries[column], errors="coerce"
    )

eligible = deliveries.loc[
    deliveries["delivery_status"].eq("DELIVERED")
    & deliveries["dispatch_date"].notna()
    & deliveries["expected_delivery_date"].notna()
    & deliveries["actual_delivery_date"].notna()
    & (
        deliveries["actual_delivery_date"]
        >= deliveries["dispatch_date"]
    )
].copy()

eligible["duration_days"] = (
    eligible["actual_delivery_date"]
    - eligible["dispatch_date"]
).dt.days

eligible["on_time"] = (
    eligible["actual_delivery_date"]
    <= eligible["expected_delivery_date"]
)

expected_count = len(eligible)
expected_on_time = int(eligible["on_time"].sum())
expected_late = expected_count - expected_on_time

expected_rate = (
    expected_on_time / expected_count * 100
    if expected_count else 0
)

expected_duration = (
    eligible["duration_days"].mean()
    if expected_count else 0
)

# Read the single-row Gold KPI
actual = gold.iloc[0]

checks = {
    "Gold contains one KPI row": len(gold) == 1,
    "Eligible count reconciles": (
        actual["eligible_deliveries"] == expected_count
    ),
    "On-time count reconciles": (
        actual["on_time_deliveries"] == expected_on_time
    ),
    "Late count reconciles": (
        actual["late_deliveries"] == expected_late
    ),
    "On-time plus late equals eligible": (
        actual["on_time_deliveries"]
        + actual["late_deliveries"]
        == actual["eligible_deliveries"]
    ),
    "On-time rate reconciles": abs(
        actual["on_time_delivery_pct"] - round(expected_rate, 2)
    ) < 0.001,
    "Average duration reconciles": abs(
        actual["average_delivery_duration_days"]
        - round(expected_duration, 2)
    ) < 0.001,
}

print("\n========== DELIVERY RECONCILIATION ==========")
print(f"Expected eligible deliveries: {expected_count}")
print(f"Expected on-time deliveries: {expected_on_time}")
print(f"Expected late deliveries: {expected_late}")
print(f"Expected on-time rate: {expected_rate:.2f}%")
print(f"Expected average duration: {expected_duration:.2f} days")

print("\n========== GOLD DELIVERY VALIDATION ==========")

for name, passed in checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")

print(f"\nPassed: {sum(checks.values())}/{len(checks)}")

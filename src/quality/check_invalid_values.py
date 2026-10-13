
from pathlib import Path
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_DIR = PROJECT_ROOT / "data_lake" / "bronze"

# Read the Bronze order items
items = pd.read_parquet(BRONZE_DIR / "order_items.parquet")

print("Total order-item rows:", len(items))

# Check missing values
print("\nMissing quantity:", items["quantity"].isna().sum())
print("Missing unit price:", items["unit_price"].isna().sum())

# Check business-rule violations
invalid_quantity = items[
    items["quantity"].notna()
    & (items["quantity"] <= 0)
]

invalid_price = items[
    items["unit_price"].notna()
    & (items["unit_price"] < 0)
]

print("\nInvalid quantity rows:", len(invalid_quantity))
print("Invalid unit-price rows:", len(invalid_price))

print("\nRows with invalid quantities:")
print(invalid_quantity.to_string(index=False))

print("\nRows with invalid prices:")
print(invalid_price.to_string(index=False))

invalid_any = items[
    (items["quantity"] <= 0)
    | (items["unit_price"] < 0)
]

print("\nUnique rows violating either rule:", len(invalid_any))
print(
    invalid_any[
        ["order_item_id", "quantity", "unit_price"]
    ].head(10)
)

invalid_discount = items[
    items["discount_pct"].notna()
    & (
        (items["discount_pct"] < 0)
        | (items["discount_pct"] > 100)
    )
]

print("\nInvalid discount rows:", len(invalid_discount))
print(invalid_discount.to_string(index=False))


orders = pd.read_parquet(BRONZE_DIR / "orders.parquet")

valid_statuses = [
    "PLACED",
    "PENDING",
    "CONFIRMED",
    "SHIPPED",
    "DELIVERED",
    "CANCELLED",
    "RETURNED",
]

invalid_status = orders[
    orders["order_status"].isna()
    | ~orders["order_status"].isin(valid_statuses)
]

print("\nInvalid order-status rows:", len(invalid_status))

print("\nUnexpected status values and counts:")
print(invalid_status["order_status"].value_counts(dropna=False))

print("\nSample invalid records:")
print(invalid_status.head(10).to_string(index=False))


orders["order_date_parsed"] = pd.to_datetime(
    orders["order_date"],
    errors="coerce"
)

invalid_dates = orders[orders["order_date_parsed"].isna()]

print("Invalid order dates:", len(invalid_dates))
print(invalid_dates[["order_id", "order_date"]].head(10))


deliveries = pd.read_parquet(BRONZE_DIR / "deliveries.parquet")

date_columns = [
    "dispatch_date",
    "expected_delivery_date",
    "actual_delivery_date",
]

for column in date_columns:
    deliveries[column + "_parsed"] = pd.to_datetime(
        deliveries[column],
        errors="coerce"
    )

    print(
        f"{column} — missing or invalid:",
        deliveries[column + "_parsed"].isna().sum()
    )


invalid_sequence = deliveries[
    deliveries["actual_delivery_date_parsed"].notna()
    & deliveries["dispatch_date_parsed"].notna()
    & (
        deliveries["actual_delivery_date_parsed"]
        < deliveries["dispatch_date_parsed"]
    )
]

print("\nActual delivery before dispatch:", len(invalid_sequence))

print(
    invalid_sequence[
        [
            "delivery_id",
            "order_id",
            "dispatch_date",
            "actual_delivery_date",
            "delivery_status",
        ]
    ].head(10).to_string(index=False)
)


missing_actual = deliveries[
    deliveries["actual_delivery_date_parsed"].isna()
]

print("\nMissing actual delivery dates by status:")
print(
    missing_actual["delivery_status"]
    .value_counts(dropna=False)
)

print("\nTotal records with missing actual delivery dates:",
      len(missing_actual))


print("\nAll delivery statuses:")
print(
    deliveries["delivery_status"]
    .value_counts(dropna=False)
)


late_deliveries = deliveries[
    deliveries["actual_delivery_date_parsed"].notna()
    & deliveries["expected_delivery_date_parsed"].notna()
    & (
        deliveries["actual_delivery_date_parsed"]
        > deliveries["expected_delivery_date_parsed"]
    )
]

print("\nLate deliveries:", len(late_deliveries))

print(
    late_deliveries[
        [
            "delivery_id",
            "order_id",
            "expected_delivery_date",
            "actual_delivery_date",
            "delivery_status",
        ]
    ].head(10).to_string(index=False)
)


# Eligible records: valid delivered records with both dates
valid_delivered = deliveries[
    (deliveries["delivery_status"] == "DELIVERED")
    & deliveries["actual_delivery_date_parsed"].notna()
    & deliveries["expected_delivery_date_parsed"].notna()
    & (
        deliveries["actual_delivery_date_parsed"]
        >= deliveries["dispatch_date_parsed"]
    )
]

# On-time deliveries: actual date is on or before expected date
on_time_deliveries = valid_delivered[
    valid_delivered["actual_delivery_date_parsed"]
    <= valid_delivered["expected_delivery_date_parsed"]
]

total_eligible = valid_delivered.shape[0]
total_on_time = on_time_deliveries.shape[0]

on_time_percentage = (
    total_on_time / total_eligible * 100
    if total_eligible > 0 else 0
)

print("\nEligible delivered records:", total_eligible)
print("On-time deliveries:", total_on_time)
print(f"On-time delivery percentage: {on_time_percentage:.2f}%")

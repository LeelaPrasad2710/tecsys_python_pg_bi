
from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
SILVER = ROOT / "data_lake" / "silver"
GOLD = ROOT / "data_lake" / "gold"

customers = pd.read_parquet(SILVER / "customers.parquet")
orders = pd.read_parquet(SILVER / "orders.parquet")
items = pd.read_parquet(SILVER / "order_items.parquet")
gold = pd.read_parquet(GOLD / "gold_customer_analytics.parquet")

# Independently calculate expected customer metrics.
delivered = orders.loc[
    orders["order_status"].eq("DELIVERED"),
    ["order_id", "customer_id"]
].copy()

expected_orders = (
    delivered.groupby("customer_id")["order_id"]
    .nunique()
    .rename("expected_order_count")
)

joined = items.merge(
    delivered,
    on="order_id",
    how="inner",
    validate="many_to_one"
)

for col in ["quantity", "unit_price", "discount_pct"]:
    joined[col] = pd.to_numeric(joined[col], errors="coerce")

joined["discount_pct"] = joined["discount_pct"].fillna(0)

joined["line_sales"] = (
    joined["quantity"]
    * joined["unit_price"]
    * (1 - joined["discount_pct"] / 100)
)

expected_purchases = joined.groupby("customer_id").agg(
    expected_units=("quantity", "sum"),
    expected_sales=("line_sales", "sum")
)

expected = customers[["customer_id"]].copy()
expected = expected.merge(
    expected_orders,
    on="customer_id",
    how="left",
    validate="one_to_one"
)
expected = expected.merge(
    expected_purchases,
    on="customer_id",
    how="left",
    validate="one_to_one"
)

expected["expected_order_count"] = (
    expected["expected_order_count"].fillna(0).astype("int64")
)
expected["expected_units"] = (
    expected["expected_units"].fillna(0).astype("int64")
)
expected["expected_sales"] = (
    expected["expected_sales"].fillna(0).round(2)
)
expected["expected_aov"] = (
    expected["expected_sales"]
    .div(expected["expected_order_count"].replace(0, float("nan")))
    .fillna(0)
    .round(2)
)

actual = gold.merge(
    expected,
    on="customer_id",
    how="left",
    validate="one_to_one"
)

checks = {
    "Gold has one row per Silver customer": (
        len(gold) == len(customers)
        and gold["customer_id"].nunique() == len(customers)
    ),
    "No missing customer IDs": gold["customer_id"].notna().all(),
    "Customer order counts reconcile": (
        actual["delivered_order_count"].eq(
            actual["expected_order_count"]
        ).all()
    ),
    "Customer units reconcile": (
        actual["units_purchased"].eq(actual["expected_units"]).all()
    ),
    "Customer net sales reconcile": (
        (actual["net_sales"] - actual["expected_sales"]).abs().le(0.01).all()
    ),
    "Average order value reconciles": (
        (actual["average_order_value"] - actual["expected_aov"])
        .abs().le(0.01).all()
    ),
    "No negative customer sales": gold["net_sales"].ge(0).all(),
    "Zero-order customers have zero average order value": (
        gold.loc[
            gold["delivered_order_count"].eq(0),
            "average_order_value"
        ].eq(0).all()
    ),
}

print("========== GOLD CUSTOMER VALIDATION ==========")
for name, passed in checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")

print(f"\nPassed: {sum(checks.values())}/{len(checks)}")

if not all(checks.values()):
    raise SystemExit("Customer Gold validation failed.")

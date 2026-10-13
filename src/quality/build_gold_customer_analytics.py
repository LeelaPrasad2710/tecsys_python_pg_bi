
from pathlib import Path
import pandas as pd

# --------------------------------------------------
# 1. Define project paths
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]

SILVER_DIR = ROOT / "data_lake" / "silver"
GOLD_DIR = ROOT / "data_lake" / "gold"

GOLD_DIR.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------
# 2. Load Silver datasets
# --------------------------------------------------
customers = pd.read_parquet(SILVER_DIR / "customers.parquet")
orders = pd.read_parquet(SILVER_DIR / "orders.parquet")
items = pd.read_parquet(SILVER_DIR / "order_items.parquet")

print("========== SOURCE SUMMARY ==========")
print(f"Silver customers: {len(customers)}")
print(f"Silver orders: {len(orders)}")
print(f"Silver order items: {len(items)}")


# --------------------------------------------------
# 3. Select delivered orders
# --------------------------------------------------
delivered_orders = orders.loc[
    orders["order_status"].eq("DELIVERED")
].copy()

print(f"Delivered orders: {len(delivered_orders)}")


# --------------------------------------------------
# 4. Calculate delivered order count per customer
# --------------------------------------------------
customer_order_counts = (
    delivered_orders
    .groupby("customer_id", as_index=False)
    .agg(
        delivered_order_count=("order_id", "nunique")
    )
)


# --------------------------------------------------
# 5. Join delivered orders to their order items
# --------------------------------------------------
delivered_items = items.merge(
    delivered_orders[["order_id", "customer_id"]],
    on="order_id",
    how="inner",
    validate="many_to_one"
)

# Convert calculation columns to numeric
for column in ["quantity", "unit_price", "discount_pct"]:
    delivered_items[column] = pd.to_numeric(
        delivered_items[column],
        errors="coerce"
    )

# Business assumption:
# A missing discount is treated as 0% discount.
delivered_items["discount_pct"] = (
    delivered_items["discount_pct"].fillna(0)
)

# Net sales after discount
delivered_items["line_net_sales"] = (
    delivered_items["quantity"]
    * delivered_items["unit_price"]
    * (1 - delivered_items["discount_pct"] / 100)
)

print(f"Matched delivered item rows: {len(delivered_items)}")


# --------------------------------------------------
# 6. Aggregate purchases per customer
# --------------------------------------------------
customer_purchases = (
    delivered_items
    .groupby("customer_id", as_index=False)
    .agg(
        units_purchased=("quantity", "sum"),
        net_sales=("line_net_sales", "sum")
    )
)


# --------------------------------------------------
# 7. Build Gold customer dataset
# --------------------------------------------------
gold = customers.merge(
    customer_order_counts,
    on="customer_id",
    how="left",
    validate="one_to_one"
)

gold = gold.merge(
    customer_purchases,
    on="customer_id",
    how="left",
    validate="one_to_one"
)

# Customers without delivered purchases receive zero activity.
for column in [
    "delivered_order_count",
    "units_purchased",
    "net_sales"
]:
    gold[column] = gold[column].fillna(0)

gold["delivered_order_count"] = (
    gold["delivered_order_count"].astype("int64")
)

gold["units_purchased"] = (
    gold["units_purchased"].astype("int64")
)

gold["net_sales"] = gold["net_sales"].round(2)

# Average Order Value = Net Sales / Delivered Order Count
# Customers with zero delivered orders receive an AOV of zero.
gold["average_order_value"] = (
    gold["net_sales"]
    .div(
        gold["delivered_order_count"].replace(
            0, float("nan")
        )
    )
    .fillna(0)
    .round(2)
)

# Highest-spending customers receive rank 1.
gold["customer_sales_rank"] = (
    gold["net_sales"]
    .rank(method="dense", ascending=False)
    .astype("int64")
)


# --------------------------------------------------
# 8. Save Gold dataset
# --------------------------------------------------
output_path = GOLD_DIR / "gold_customer_analytics.parquet"

gold.to_parquet(output_path, index=False)


# --------------------------------------------------
# 9. Print summary and top customers
# --------------------------------------------------
print("\n========== GOLD CUSTOMER ANALYTICS ==========")
print(f"Gold customer rows: {len(gold)}")

print(
    "Customers with delivered orders:",
    int((gold["delivered_order_count"] > 0).sum())
)

print(
    "Customers without delivered orders:",
    int((gold["delivered_order_count"] == 0).sum())
)

print(
    "Total delivered orders:",
    gold["delivered_order_count"].sum()
)

print(
    "Total units purchased:",
    gold["units_purchased"].sum()
)

print(
    "Total net sales:",
    round(gold["net_sales"].sum(), 2)
)

print("\nTop 10 customers by net sales:")

print(
    gold.sort_values(
        ["net_sales", "customer_id"],
        ascending=[False, True]
    )[
        [
            "customer_id",
            "customer_name",
            "city",
            "delivered_order_count",
            "units_purchased",
            "net_sales",
            "average_order_value",
            "customer_sales_rank"
        ]
    ]
    .head(10)
    .to_string(index=False)
)

print(f"\nSaved Gold dataset: {output_path}")

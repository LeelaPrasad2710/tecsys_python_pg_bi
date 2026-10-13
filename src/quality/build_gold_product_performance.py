
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
# 2. LOAD SILVER TABLES
# ==================================================
orders = pd.read_parquet(SILVER / "orders.parquet")
items = pd.read_parquet(SILVER / "order_items.parquet")
products = pd.read_parquet(SILVER / "products.parquet")

# ==================================================
# 3. FILTER DELIVERED ORDERS
# ==================================================
delivered_orders = orders.loc[
    orders["order_status"].eq("DELIVERED")
].copy()

# ==================================================
# 4. JOIN ORDER ITEMS TO ORDERS
# ==================================================
sales = items.merge(
    delivered_orders[["order_id", "order_status"]],
    on="order_id",
    how="inner",
    validate="many_to_one",
)

# ==================================================
# 5. JOIN PRODUCTS TO GET PRODUCT DETAILS
# ==================================================
sales = sales.merge(
    products[["product_id", "product_name", "category"]],
    on="product_id",
    how="inner",
    validate="many_to_one",
)

# ==================================================
# 6. CALCULATE SALES AFTER DISCOUNTS
# ==================================================
sales["net_line_sales"] = (
    sales["quantity"]
    * sales["unit_price"]
    * (1 - sales["discount_pct"] / 100)
)

# ==================================================
# 7. AGGREGATE BY PRODUCT
# ==================================================
gold_product = (
    sales.groupby(
        ["product_id", "product_name", "category"],
        as_index=False,
    )
    .agg(
        units_sold=("quantity", "sum"),
        delivered_orders=("order_id", "nunique"),
        net_sales=("net_line_sales", "sum"),
    )
)

gold_product["net_sales"] = (
    gold_product["net_sales"].round(2)
)

# Rank products by net sales.
# Rank 1 represents the highest-revenue product.
gold_product["revenue_rank"] = (
    gold_product["net_sales"]
    .rank(method="dense", ascending=False)
    .astype(int)
)

gold_product = gold_product.sort_values(
    ["revenue_rank", "product_id"]
).reset_index(drop=True)

# ==================================================
# 8. SAVE GOLD DATASET
# ==================================================
output_path = GOLD / "gold_product_performance.parquet"

gold_product.to_parquet(output_path, index=False)

# ==================================================
# 9. PRINT SUMMARY
# ==================================================
print("\n========== GOLD PRODUCT PERFORMANCE ==========")

print(f"Delivered orders: {delivered_orders['order_id'].nunique()}")
print(f"Matched order-item rows: {len(sales)}")
print(f"Products with sales: {gold_product['product_id'].nunique()}")
print(f"Total units sold: {gold_product['units_sold'].sum()}")
print(f"Total net sales: {gold_product['net_sales'].sum():,.2f}")

print("\nTop 10 products by net sales:")
print(
    gold_product[
        [
            "revenue_rank",
            "product_id",
            "product_name",
            "category",
            "units_sold",
            "delivered_orders",
            "net_sales",
        ]
    ].head(10).to_string(index=False)
)

print("\nRevenue by category:")
print(
    gold_product.groupby("category")["net_sales"]
    .sum()
    .sort_values(ascending=False)
    .round(2)
    .to_string()
)

print(f"\nGold saved to: {output_path}")
print("\nGold product performance build completed.")

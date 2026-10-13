
from pathlib import Path
import pandas as pd

# ==================================================
# 1. PROJECT PATHS
# ==================================================
ROOT = Path(__file__).resolve().parents[2]

SILVER = ROOT / "data_lake" / "silver"
GOLD = ROOT / "data_lake" / "gold"

# ==================================================
# 2. LOAD DATA
# ==================================================
orders = pd.read_parquet(SILVER / "orders.parquet")
items = pd.read_parquet(SILVER / "order_items.parquet")
products = pd.read_parquet(SILVER / "products.parquet")

gold_product = pd.read_parquet(
    GOLD / "gold_product_performance.parquet"
)

gold_daily = pd.read_parquet(
    GOLD / "gold_daily_sales.parquet"
)

# ==================================================
# 3. INDEPENDENTLY REBUILD EXPECTED PRODUCT METRICS
# ==================================================
delivered_orders = orders.loc[
    orders["order_status"].eq("DELIVERED")
].copy()

sales = items.merge(
    delivered_orders[["order_id"]],
    on="order_id",
    how="inner",
    validate="many_to_one",
)

sales = sales.merge(
    products[["product_id", "product_name", "category"]],
    on="product_id",
    how="inner",
    validate="many_to_one",
)

sales["line_sales"] = (
    sales["quantity"]
    * sales["unit_price"]
    * (1 - sales["discount_pct"] / 100)
)

expected_product = (
    sales.groupby(
        ["product_id", "product_name", "category"],
        as_index=False,
    )
    .agg(
        units_sold=("quantity", "sum"),
        delivered_orders=("order_id", "nunique"),
        net_sales=("line_sales", "sum"),
    )
)

expected_product["net_sales"] = (
    expected_product["net_sales"].round(2)
)

# ==================================================
# 4. VALIDATION CHECKS
# ==================================================

# Check 1: Product IDs must be unique
check_unique_products = (
    gold_product["product_id"].notna().all()
    and gold_product["product_id"].is_unique
)

# Check 2: Revenue ranks must be positive integers
check_valid_ranks = (
    gold_product["revenue_rank"].notna().all()
    and gold_product["revenue_rank"].ge(1).all()
    and pd.api.types.is_integer_dtype(
        gold_product["revenue_rank"]
    )
)

# Check 3: Units sold must be non-negative
check_units_nonnegative = (
    gold_product["units_sold"].ge(0).all()
)

# Check 4: Net sales must be non-negative
check_sales_nonnegative = (
    gold_product["net_sales"].ge(0).all()
)

# Check 5: Product-level units reconcile
check_units_reconcile = (
    gold_product["units_sold"].sum()
    == expected_product["units_sold"].sum()
)

# Check 6: Product-level revenue reconciles
actual = gold_product[
    ["product_id", "units_sold", "delivered_orders", "net_sales"]
].sort_values("product_id").reset_index(drop=True)

expected = expected_product[
    ["product_id", "units_sold", "delivered_orders", "net_sales"]
].sort_values("product_id").reset_index(drop=True)

same_product_ids = actual["product_id"].equals(
    expected["product_id"]
)

same_product_metrics = (
    same_product_ids
    and actual["units_sold"].equals(expected["units_sold"])
    and actual["delivered_orders"].equals(
        expected["delivered_orders"]
    )
    and actual["net_sales"].equals(expected["net_sales"])
)

# Check 7: Revenue ranking agrees with sales
expected_ranks = (
    gold_product["net_sales"]
    .rank(method="dense", ascending=False)
    .astype(int)
)

check_rank_order = (
    gold_product["revenue_rank"].reset_index(drop=True)
    .equals(expected_ranks.reset_index(drop=True))
)

# Check 8: Product revenue should be close to Daily Sales.
# The two datasets round at different aggregation levels.
product_total = gold_product["net_sales"].sum()
daily_total = gold_daily["net_sales"].sum()
rounding_difference = product_total - daily_total

# Maximum conservative rounding allowance:
# half a cent per product plus half a cent per date.
rounding_tolerance = (
    0.005 * (
        gold_product["product_id"].nunique()
        + gold_daily["order_date"].nunique()
    )
    + 0.000001
)

check_cross_dataset_sales = (
    abs(rounding_difference) <= rounding_tolerance
)

# ==================================================
# 5. DISPLAY RECONCILIATION DETAILS
# ==================================================
print("\n========== PRODUCT SALES RECONCILIATION ==========")
print(f"Products in Gold: {gold_product['product_id'].nunique()}")
print(f"Expected products with sales: {len(expected_product)}")
print(f"Product Gold units: {gold_product['units_sold'].sum()}")
print(f"Expected units: {expected_product['units_sold'].sum()}")
print(f"Product Gold net sales: {product_total:,.2f}")
print(f"Daily Gold net sales: {daily_total:,.2f}")
print(f"Sales difference: {rounding_difference:,.2f}")
print(f"Rounding tolerance: {rounding_tolerance:,.2f}")

# ==================================================
# 6. PRINT ALL VALIDATION RESULTS
# ==================================================
checks = {
    "Product IDs are unique": check_unique_products,
    "Revenue ranks are valid": check_valid_ranks,
    "Units are non-negative": check_units_nonnegative,
    "Sales are non-negative": check_sales_nonnegative,
    "Product units reconcile": check_units_reconcile,
    "Product metrics reconcile to Silver": same_product_metrics,
    "Revenue ranks match sales": check_rank_order,
    "Product sales reconcile with Daily Sales": check_cross_dataset_sales,
}

print("\n========== GOLD PRODUCT VALIDATION ==========")

for name, passed in checks.items():
    print(f"{name}: {'PASS' if passed else 'FAIL'}")

passed_count = sum(checks.values())
print(f"\nPassed: {passed_count}/{len(checks)}")

if passed_count == len(checks):
    print("All Gold Product Performance validations passed.")
else:
    print("Investigate the failed checks before proceeding.")

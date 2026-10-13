
import pandas as pd
from pathlib import Path

root = Path("data_lake")

bronze = pd.read_parquet(root / "bronze" / "order_items.parquet")
silver = pd.read_parquet(root / "silver" / "order_items.parquet")
quarantine = pd.read_parquet(
    root / "quarantine" / "order_items_issues.parquet"
)

# Compare the source IDs with the IDs in both output datasets.
silver_ids = set(silver["order_item_id"].dropna())
quarantine_ids = set(quarantine["order_item_id"].dropna())

missing = bronze[
    ~bronze["order_item_id"].isin(silver_ids | quarantine_ids)
]

print("Bronze rows:", len(bronze))
print("Silver rows:", len(silver))
print("Quarantine rows:", len(quarantine))
print("Rows with IDs in neither output:", len(missing))

print("\nRecords whose IDs appear in neither output:")
print(missing.to_string(index=False))

# Check repeated IDs in Bronze and their distribution in the outputs.
duplicate_mask = bronze["order_item_id"].duplicated(keep=False)
duplicate_ids = bronze.loc[duplicate_mask, "order_item_id"].dropna().unique()

print("\nDuplicate IDs in Bronze:")
for item_id in duplicate_ids:
    print(
        item_id,
        "| Bronze:", int((bronze["order_item_id"] == item_id).sum()),
        "| Silver:", int((silver["order_item_id"] == item_id).sum()),
        "| Quarantine:", int((quarantine["order_item_id"] == item_id).sum())
    )

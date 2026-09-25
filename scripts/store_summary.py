import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Count records for each store
store_summary = df.groupby("Store").size().reset_index(name="RecordCount")

print("Store-wise GPS record summary:")
print(store_summary)

print("\nTotal stores:", store_summary["Store"].nunique())
print("Total GPS records:", store_summary["RecordCount"].sum())
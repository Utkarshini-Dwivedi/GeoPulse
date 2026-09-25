import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Convert timestamp to datetime
df["Timestamp"] = pd.to_datetime(df["Timestamp"])

# Extract hour
df["Hour"] = df["Timestamp"].dt.hour

# Count GPS records by hour
hourly_summary = (
    df.groupby("Hour")
    .size()
    .reset_index(name="RecordCount")
)

print("Hour-wise GPS record summary:")
print(hourly_summary)

print("\nTotal hours covered:", hourly_summary["Hour"].nunique())
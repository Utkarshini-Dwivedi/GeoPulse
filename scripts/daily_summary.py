import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Convert timestamp to datetime
df["Timestamp"] = pd.to_datetime(df["Timestamp"])

# Extract date
df["Date"] = df["Timestamp"].dt.date

# Count records for each date
daily_summary = df.groupby("Date").size().reset_index(name="RecordCount")

print("Date-wise GPS record summary:")
print(daily_summary)

print("\nTotal days:", daily_summary["Date"].nunique())
print("Total GPS records:", daily_summary["RecordCount"].sum())
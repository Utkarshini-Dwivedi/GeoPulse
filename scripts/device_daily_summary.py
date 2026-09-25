import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Convert timestamp to datetime
df["Timestamp"] = pd.to_datetime(df["Timestamp"])

# Extract date
df["Date"] = df["Timestamp"].dt.date

# Count records by device and date
device_daily_summary = (
    df.groupby(["DeviceID", "Date"])
    .size()
    .reset_index(name="RecordCount")
)

print("Device-wise daily GPS summary:")
print(device_daily_summary)

print("\nTotal device-day combinations:", len(device_daily_summary))

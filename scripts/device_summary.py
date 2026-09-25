import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Count records for each device
device_summary = df.groupby("DeviceID").size().reset_index(name="RecordCount")

print("Device-wise GPS record summary:")
print(device_summary)

print("\nTotal devices:", device_summary["DeviceID"].nunique())
print("Minimum records per device:", device_summary["RecordCount"].min())
print("Maximum records per device:", device_summary["RecordCount"].max())
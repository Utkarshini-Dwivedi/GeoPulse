import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Convert timestamp to datetime
df["Timestamp"] = pd.to_datetime(df["Timestamp"])

# Find earliest and latest timestamps
start_time = df["Timestamp"].min()
end_time = df["Timestamp"].max()

print("GPS data time range:")
print("Start:", start_time)
print("End:", end_time)

print("\nTotal duration:")
print(end_time - start_time)
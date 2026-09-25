import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Convert Timestamp column to datetime
df["Timestamp"] = pd.to_datetime(df["Timestamp"], errors="coerce")

# Check invalid timestamps
invalid_timestamps = df["Timestamp"].isna().sum()

print("Total records:", len(df))
print("Invalid timestamps:", invalid_timestamps)

if invalid_timestamps == 0:
    print("Timestamp validation passed successfully!")
else:
    print("Warning: Invalid timestamps found.")
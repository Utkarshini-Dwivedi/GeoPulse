import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Convert timestamp to datetime
df["Timestamp"] = pd.to_datetime(df["Timestamp"])

# Extract hour
df["Hour"] = df["Timestamp"].dt.hour

# Classify traffic period
def classify_period(hour):
    if 6 <= hour < 12:
        return "Morning"
    elif 12 <= hour < 17:
        return "Afternoon"
    elif 17 <= hour < 21:
        return "Evening"
    else:
        return "Night"

df["TrafficPeriod"] = df["Hour"].apply(classify_period)

# Store + traffic period summary
summary = (
    df.groupby(["Store", "TrafficPeriod"])
    .size()
    .reset_index(name="RecordCount")
)

print("Store-wise traffic period summary:")
print(summary)

print("\nTotal records:", summary["RecordCount"].sum())
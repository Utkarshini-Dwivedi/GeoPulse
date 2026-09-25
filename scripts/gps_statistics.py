import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Basic GPS statistics
print("=== GPS Coordinate Statistics ===")

print("\nLatitude statistics:")
print(df["Latitude"].describe())

print("\nLongitude statistics:")
print(df["Longitude"].describe())

print("\nTotal GPS records:", len(df))
print("Total devices:", df["DeviceID"].nunique())
print("Total stores:", df["Store"].nunique())
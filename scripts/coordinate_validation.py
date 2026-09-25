import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Check that coordinates are numeric
df["Latitude"] = pd.to_numeric(df["Latitude"], errors="coerce")
df["Longitude"] = pd.to_numeric(df["Longitude"], errors="coerce")

invalid_latitude = df["Latitude"].isna().sum()
invalid_longitude = df["Longitude"].isna().sum()

# Check valid geographic ranges
out_of_range_latitude = ((df["Latitude"] < -90) | (df["Latitude"] > 90)).sum()
out_of_range_longitude = ((df["Longitude"] < -180) | (df["Longitude"] > 180)).sum()

print("Invalid latitude values:", invalid_latitude)
print("Invalid longitude values:", invalid_longitude)
print("Out-of-range latitude values:", out_of_range_latitude)
print("Out-of-range longitude values:", out_of_range_longitude)

if (
    invalid_latitude == 0
    and invalid_longitude == 0
    and out_of_range_latitude == 0
    and out_of_range_longitude == 0
):
    print("\nCoordinate validation passed successfully!")
else:
    print("\nWarning: Invalid GPS coordinates found.")
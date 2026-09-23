import pandas as pd

# Load synthetic GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Basic dataset checks
print("Dataset shape:", df.shape)
print("\nMissing values:")
print(df.isnull().sum())

print("\nDuplicate rows:", df.duplicated().sum())

# GPS coordinate validation
print("\nLatitude range:", df["Latitude"].min(), "to", df["Latitude"].max())
print("Longitude range:", df["Longitude"].min(), "to", df["Longitude"].max())

# Store validation
print("\nStore counts:")
print(df["Store"].value_counts())

print("\nValidation completed successfully!")
import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

print("=== GeoPulse Dataset Preview ===")

print("\nFirst 5 records:")
print(df.head())

print("\nLast 5 records:")
print(df.tail())

print("\nDataset shape:", df.shape)
print("\nData types:")
print(df.dtypes)
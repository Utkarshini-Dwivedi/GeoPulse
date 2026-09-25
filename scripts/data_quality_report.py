import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

print("=== GeoPulse Data Quality Report ===")

# Dataset size
print("\nRows:", df.shape[0])
print("Columns:", df.shape[1])

# Missing values
missing_values = df.isnull().sum()
print("\nMissing values:")
print(missing_values)

# Duplicate rows
duplicate_rows = df.duplicated().sum()
print("\nDuplicate rows:", duplicate_rows)

# Column names
print("\nColumns:")
print(df.columns.tolist())

# Overall status
if missing_values.sum() == 0 and duplicate_rows == 0:
    print("\nData quality check: PASSED")
else:
    print("\nData quality check: REVIEW REQUIRED")
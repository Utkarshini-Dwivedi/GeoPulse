import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Count unique devices for each store
store_devices = (
    df.groupby("Store")["DeviceID"]
    .nunique()
    .reset_index(name="UniqueDevices")
)

print("Store-wise unique device summary:")
print(store_devices)

print("\nTotal unique devices:", df["DeviceID"].nunique())
import pandas as pd

# Load GPS data
df = pd.read_csv("data/raw/synthetic_gps_data.csv")

# Find devices that visited each store
store_a_devices = set(df.loc[df["Store"] == "Store_A", "DeviceID"])
store_b_devices = set(df.loc[df["Store"] == "Store_B", "DeviceID"])

# Find devices present at both stores
shared_devices = store_a_devices.intersection(store_b_devices)

print("Store A unique devices:", len(store_a_devices))
print("Store B unique devices:", len(store_b_devices))
print("Shared devices:", len(shared_devices))

print("\nShared Device IDs:")
print(sorted(shared_devices))
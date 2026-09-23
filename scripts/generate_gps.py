import csv
import random
from datetime import datetime, timedelta

# Reproducible random data
random.seed(42)

# Store locations
stores = {
    "Store_A": (28.6139, 77.2090),
    "Store_B": (28.6160, 77.2130)
}

# Generate synthetic GPS data
rows = []

num_devices = 7
days = 7

start_time = datetime(2026, 9, 1, 6, 0, 0)

for device_num in range(1, num_devices + 1):

    device_id = f"DEV_{device_num:04d}"

    current_time = start_time

    for day in range(days):

        # Each device generates 40 GPS points per day
        for _ in range(40):

            # Randomly choose a nearby store/location
            store_name = random.choice(["Store_A", "Store_B"])

            base_lat, base_lon = stores[store_name]

            # Small random movement around the store
            latitude = base_lat + random.uniform(-0.004, 0.004)
            longitude = base_lon + random.uniform(-0.004, 0.004)

            timestamp = current_time + timedelta(
                minutes=random.randint(1, 30)
            )

            rows.append({
                "DeviceID": device_id,
                "Latitude": round(latitude, 6),
                "Longitude": round(longitude, 6),
                "Timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S"),
                "Store": store_name
            })

            current_time = timestamp

        # Move to next day
        current_time = start_time + timedelta(days=day + 1)

# Save CSV
output_file = "data/raw/synthetic_gps_data.csv"

with open(output_file, "w", newline="") as file:
    writer = csv.DictWriter(
        file,
        fieldnames=[
            "DeviceID",
            "Latitude",
            "Longitude",
            "Timestamp",
            "Store"
        ]
    )

    writer.writeheader()
    writer.writerows(rows)

print(f"Generated {len(rows)} GPS records.")
print(f"Saved to {output_file}")

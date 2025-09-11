import pandas as pd
from datetime import datetime, timedelta

# Constants
start_time = datetime(2025, 1, 1, 15, 0, 0)  # Start at 3:00 PM
readings_per_minute = 1
total_minutes = 240
stall_start_temp = 150.0
stall_end_temp = 170.0
final_temp = 205.0
stall_start_time = 180
stall_duration = 60

# Machines
machines = ["GreenEgg 1", "GreenEgg 2", "Traeger"]

# Generate time intervals
timestamps = [start_time + timedelta(minutes=i) for i in range(total_minutes)]

# Temperature progression
temperatures = []
current_temp = 70.0
for i in range(total_minutes):
    if i < stall_start_time:
        current_temp += 0.4
    elif stall_start_time <= i < stall_start_time + stall_duration:
        current_temp += (0.2 if i % 2 == 0 else -0.2)
    else:
        current_temp += 0.5
    temperatures.append(round(current_temp, 1))

# Assign machine IDs (round robin)
machine_ids = [machines[i % len(machines)] for i in range(total_minutes)]

# Create the DataFrame
data = pd.DataFrame({
    "timestamp": timestamps,
    "machine_id": machine_ids,
    "temperature": temperatures
})

# Save to CSV
csv_file_path = "data/smoker_temps.csv"
data.to_csv(csv_file_path, index=False)
print(f"CSV written to {csv_file_path}")
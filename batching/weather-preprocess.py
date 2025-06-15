from pathlib import Path
import pandas as pd
import yaml

current_path = Path(__file__).parent.resolve()

with open(current_path / "./config.yaml", "r") as f:
    config = yaml.safe_load(f)

date_1 = config["split"]["date_1"]
date_2 = config["split"]["date_2"]
weather_dataset_path = config["data"]["weather"]
output_path = config["output"]

if not date_1 or not date_2 or not weather_dataset_path:
    raise ValueError("Configuration file is missing required fields.")

weather_dataset = pd.read_csv(current_path / weather_dataset_path, dtype=str)
print(f"Loaded {len(weather_dataset)} weather records.")
print(weather_dataset.head())

# Convert Date column to datetime, the format is 'YYYY-MM-DD'
weather_dataset["date"] = pd.to_datetime(
    weather_dataset["date"], format="%Y-%m-%d", errors="coerce"
)

# Convert date strings from config to datetime objects
date_1 = pd.to_datetime(date_1)
date_2 = pd.to_datetime(date_2)

# Split dataset into three portions based on the dates
weather_dataset_1 = weather_dataset[weather_dataset["date"] < date_1].copy()
weather_dataset_2 = weather_dataset[weather_dataset["date"] < date_2].copy()
weather_dataset_3 = weather_dataset.copy()

print(f"Dataset 1 size: {len(weather_dataset_1)}")
print(f"Dataset 2 size: {len(weather_dataset_2)}")
print(f"Dataset 3 size: {len(weather_dataset_3)}")

# Convert Date back to original format before saving
weather_dataset_1["date"] = weather_dataset_1["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
weather_dataset_2["date"] = weather_dataset_2["date"].dt.strftime("%Y-%m-%d %H:%M:%S")
weather_dataset_3["date"] = weather_dataset_3["date"].dt.strftime("%Y-%m-%d %H:%M:%S")

# Save the datasets to CSV files
path_1 = current_path / output_path / "1"
path_2 = current_path / output_path / "2"
path_3 = current_path / output_path / "3"
path_1.mkdir(parents=True, exist_ok=True)
path_2.mkdir(parents=True, exist_ok=True)
path_3.mkdir(parents=True, exist_ok=True)
weather_dataset_1.to_csv(path_1 / "weather.csv", index=False, mode="w+")
weather_dataset_2.to_csv(path_2 / "weather.csv", index=False, mode="w+")
weather_dataset_3.to_csv(path_3 / "weather.csv", index=False, mode="w+")

from pathlib import Path
import pandas as pd
import yaml

current_path = Path(__file__).parent.resolve()

with open(current_path / "../configs/preprocessing.yaml", "r") as f:
    config = yaml.safe_load(f)

date_1 = config["split"]["date_1"]
date_2 = config["split"]["date_2"]
wb_dataset_path = config["data"]["wb"]
output_path = config["output"]

if not date_1 or not date_2 or not wb_dataset_path:
    raise ValueError("Configuration file is missing required fields.")

wb_dataset = pd.read_csv(current_path / wb_dataset_path, dtype=str)
print(f"Loaded {len(wb_dataset)} incidents.")
print(wb_dataset.head())

# Convert Year column to datetime
wb_dataset["Year"] = pd.to_datetime(wb_dataset["Year"], format="%Y", errors="coerce")

# Convert date strings from config to datetime objects
date_1 = pd.to_datetime(date_1)
date_2 = pd.to_datetime(date_2)

# Split dataset into three portions based on the dates
wb_dataset_1 = wb_dataset[wb_dataset["Year"] < date_1].copy()
wb_dataset_2 = wb_dataset[(wb_dataset["Year"] < date_2)].copy()
wb_dataset_3 = wb_dataset.copy()
print(f"Dataset 1 size: {len(wb_dataset_1)}")
print(f"Dataset 2 size: {len(wb_dataset_2)}")
print(f"Dataset 3 size: {len(wb_dataset_3)}")

# Convert Year back to original format before saving
wb_dataset_1["Year"] = wb_dataset_1["Year"].dt.strftime("%Y")
wb_dataset_2["Year"] = wb_dataset_2["Year"].dt.strftime("%Y")
wb_dataset_3["Year"] = wb_dataset_3["Year"].dt.strftime("%Y")

# Save the datasets to CSV files
path_1 = current_path / output_path / "1"
path_2 = current_path / output_path / "2"
path_3 = current_path / output_path / "3"
path_1.mkdir(parents=True, exist_ok=True)
path_2.mkdir(parents=True, exist_ok=True)
path_3.mkdir(parents=True, exist_ok=True)
wb_dataset_1.to_csv(path_1 / "well-being.csv", index=False, mode="w+")
wb_dataset_2.to_csv(path_2 / "well-being.csv", index=False, mode="w+")
wb_dataset_3.to_csv(path_3 / "well-being.csv", index=False, mode="w+")

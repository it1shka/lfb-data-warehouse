import yaml
import pandas as pd
from pathlib import Path

current_path = Path(__file__).parent.resolve()

with open(current_path / "../configs/preprocessing.yaml", "r") as f:
    config = yaml.safe_load(f)

date_1 = config["split"]["date_1"]
date_2 = config["split"]["date_2"]
lfb_dataset_new_url = config["data"]["lfb_new"]
lfb_dataset_old_path = config["data"]["lfb_old"]
output_path = config["output"]

if not date_1 or not date_2 or not lfb_dataset_new_url or not lfb_dataset_old_path:
    raise ValueError("Configuration file is missing required fields.")

lfb_new_df = pd.read_csv(current_path / lfb_dataset_new_url, dtype=str)
lfb_old_df = pd.read_csv(current_path / lfb_dataset_old_path, dtype=str)
print(f"Loaded {len(lfb_new_df)} new incidents and {len(lfb_old_df)} old incidents.")

lfb_dataset = pd.concat([lfb_new_df, lfb_old_df], ignore_index=True)

# Convert 'DateOfCall' to date. It has the format of 'dd-MMM-yy'
lfb_dataset["DateOfCall"] = pd.to_datetime(
    lfb_dataset["DateOfCall"], format="%d-%b-%y", errors="coerce"
)

# Convert date strings from config to datetime objects
date_1 = pd.to_datetime(date_1)
date_2 = pd.to_datetime(date_2)

# Split dataset into three portions based on the dates
lfb_dataset_1 = lfb_dataset[lfb_dataset["DateOfCall"] < date_1].copy()
lfb_dataset_2 = lfb_dataset[lfb_dataset["DateOfCall"] < date_2].copy()
lfb_dataset_3 = lfb_dataset.copy()

print(f"Dataset 1 size: {len(lfb_dataset_1)}")
print(f"Dataset 2 size: {len(lfb_dataset_2)}")
print(f"Dataset 3 size: {len(lfb_dataset_3)}")

# Convert DateOfCall back to original format before saving
lfb_dataset_1["DateOfCall"] = lfb_dataset_1["DateOfCall"].dt.strftime("%d-%b-%y")
lfb_dataset_2["DateOfCall"] = lfb_dataset_2["DateOfCall"].dt.strftime("%d-%b-%y")
lfb_dataset_3["DateOfCall"] = lfb_dataset_3["DateOfCall"].dt.strftime("%d-%b-%y")

# Save the datasets to CSV files
path_1 = current_path / output_path / "1"
path_2 = current_path / output_path / "2"
path_3 = current_path / output_path / "3"
path_1.mkdir(parents=True, exist_ok=True)
path_2.mkdir(parents=True, exist_ok=True)
path_3.mkdir(parents=True, exist_ok=True)

lfb_dataset_1.to_csv(path_1 / "lfb-calls.csv", index=False, mode="w+")
lfb_dataset_2.to_csv(path_2 / "lfb-calls.csv", index=False, mode="w+")
lfb_dataset_3.to_csv(path_3 / "lfb-calls.csv", index=False, mode="w+")

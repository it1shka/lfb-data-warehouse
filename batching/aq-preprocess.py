from pathlib import Path
import pandas as pd
import yaml

current_path = Path(__file__).parent.resolve()

with open(current_path / "./config.yaml", "r") as f:
    config = yaml.safe_load(f)

date_1 = config["split"]["date_1"]
date_2 = config["split"]["date_2"]
weather_dataset_path = config["data"]["aq"]["path"]
weather_dataset_names = config["data"]["aq"]["names"]
output_path = config["output"]

if not date_1 or not date_2 or not weather_dataset_names:
    raise ValueError("Configuration file is missing required fields.")

aq_dataframes = [
    pd.read_csv(current_path / weather_dataset_path / name, dtype=str)
    for name in weather_dataset_names
]
print(f"Loaded {len(aq_dataframes)} air quality datasets.")

for df in aq_dataframes:
    # Convert ReadingDateTime column to datetime, the format is 'dd/mm/yyyy hh:mm'
    df["ReadingDateTime"] = pd.to_datetime(
        df["ReadingDateTime"], format="%d/%m/%Y %H:%M", errors="coerce"
    )

# Convert date strings from config to datetime objects
date_1 = pd.to_datetime(date_1)
date_2 = pd.to_datetime(date_2)

# Split dataset into three portions based on the dates
for i, df in enumerate(aq_dataframes):
    df_1 = df[df["ReadingDateTime"] < date_1].copy()
    df_2 = df[df["ReadingDateTime"] < date_2].copy()
    df_3 = df.copy()
    print(f"Dataset 1 size: {len(df_1)}")
    print(f"Dataset 2 size: {len(df_2)}")
    print(f"Dataset 3 size: {len(df_3)}")

    # Convert ReadingDateTime back to original format before saving
    df_1["ReadingDateTime"] = df_1["ReadingDateTime"].dt.strftime("%d/%m/%Y %H:%M")
    df_2["ReadingDateTime"] = df_2["ReadingDateTime"].dt.strftime("%d/%m/%Y %H:%M")
    df_3["ReadingDateTime"] = df_3["ReadingDateTime"].dt.strftime("%d/%m/%Y %H:%M")

    # Save the datasets to CSV files
    path_1 = current_path / output_path / "1" / "air-quality"
    path_2 = current_path / output_path / "2" / "air-quality"
    path_3 = current_path / output_path / "3" / "air-quality"
    path_1.mkdir(parents=True, exist_ok=True)
    path_2.mkdir(parents=True, exist_ok=True)
    path_3.mkdir(parents=True, exist_ok=True)

    dataset_name = weather_dataset_names[i]
    df_1.to_csv(path_1 / dataset_name, index=False, mode="w+")
    df_2.to_csv(path_2 / dataset_name, index=False, mode="w+")
    df_3.to_csv(path_3 / dataset_name, index=False, mode="w+")

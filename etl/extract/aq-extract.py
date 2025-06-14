import sys
import logging
from typing import List
from pyspark.sql import SparkSession, DataFrame


FILENAMES = [
    "Air-Bexley-BelvedereWest.csv",
    "Air-Enfield-BowesPrimarySchool.csv",
    "Air-Islington-Arsenal.csv",
    "Air-KensingtonAndChelsea-NorthKen.csv",
    "Air-Wandsworth-Battersea.csv",
    "Air-Westminister-OxfordStreet.csv",
]
DATASET_NAME = "air-quality"
DATASET_EXTENSION = "parquet"
ID_COLS = ["Site", "Species", "ReadingDateTime"]


logging.basicConfig(level=logging.INFO)


def run(spark: SparkSession, config: dict) -> None:
    input_path = config["inputDatasetPath"]
    output_path = f"{config['outputDatasetPath']}/{DATASET_NAME}.{DATASET_EXTENSION}"

    dataframes: List[DataFrame] = []
    for filename in FILENAMES:
        logging.info(f"Loading file: {filename}")
        df = spark.read.csv(f"{input_path}/{filename}", header=True, inferSchema=True)
        dataframes.append(df)

    logging.info("Combining dataframes...")
    # Use unionByName for better schema handling
    combined_df = dataframes[0]
    for df in dataframes[1:]:
        combined_df = combined_df.unionByName(df)
    
    # Cache the combined dataframe since we'll use it multiple times
    combined_df.cache()
    row_count = combined_df.count()
    logging.info(f"Combined dataframe has {row_count} rows")

    try:
        logging.info("Attempting to load existing dataset...")
        existing_dataset = spark.read.parquet(output_path)
        existing_dataset.cache()  # Cache for reuse
        existing_count = existing_dataset.count()
        logging.info(f"Existing dataset loaded with {existing_count} rows")

        # More efficient duplicate detection using broadcast hint for smaller dataset
        existing_ids = existing_dataset.select(*ID_COLS).distinct()
        new_rows = combined_df.join(existing_ids.hint("broadcast"), ID_COLS, "left_anti")
        new_rows.cache()  # Cache since we'll count and potentially write
        
        new_count = new_rows.count()
        logging.info(f"Found {new_count} new rows to add")

        if new_count > 0:
            logging.info("Appending new rows to existing dataset...")
            new_rows.write.mode("append").parquet(output_path)
            logging.info(f"Successfully appended {new_count} new rows")
        else:
            logging.info("No new rows to add, dataset unchanged")

    except Exception as e:
        logging.info(f"No existing dataset found or error loading: {e}")
        logging.info("Creating initial dataset...")
        combined_df.write.mode("overwrite").parquet(output_path)
        logging.info(f"Initial dataset created successfully with {row_count} rows")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Air Quality Extract")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/data/air-quality"
        ),
        "outputDatasetPath": (
            sys.argv[1] if len(sys.argv) > 1 else "s3a://dwp/staging"
        ),
    }

    run(spark, config)

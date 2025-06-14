import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


DATASET_NAME = "lfb-calls"
DATASET_EXTENSION = "parquet"
DATE_CALL_COL = "DateOfCall"
ID_COL = "IncidentNumber"


logging.basicConfig(level=logging.INFO)


def run(spark: SparkSession, config: dict) -> None:
    output_dataset_path = (
        f"{config['outputDatasetPath']}/{DATASET_NAME}.{DATASET_EXTENSION}"
    )

    logging.info("Reading new LFB dataset...")
    lfb_dataset = spark.read.csv(
        config["inputDatasetPath"], header=True, inferSchema=True
    )
    lfb_dataset.cache()  # Cache the dataset for performance

    row_count = lfb_dataset.count()
    logging.info(f"New dataset loaded with {row_count} rows")

    logging.info("Converting DateOfCall to date format...")
    lfb_dataset = lfb_dataset.withColumn(
        DATE_CALL_COL, to_date(col(DATE_CALL_COL), "dd-MMM-yy")
    )

    try:
        logging.info("Attempting to load existing dataset...")
        existing_dataset = spark.read.parquet(output_dataset_path).cache()
        existing_count = existing_dataset.count()
        logging.info(f"Existing dataset loaded with {existing_count} rows")

        existing_ids = existing_dataset.select(ID_COL).distinct()
        new_rows = lfb_dataset.join(existing_ids, ID_COL, "left_anti")
        new_rows_count = new_rows.count()
        logging.info(f"Found {new_rows_count} new rows to add")

        if new_rows_count > 0:
            logging.info("Appending new rows to existing dataset...")
            new_rows.write.mode("append").parquet(output_dataset_path)
            logging.info(f"Successfully appended {new_rows_count} new rows")
        else:
            logging.info("No new rows to add, dataset unchanged")

    except Exception as e:
        logging.info(f"No existing dataset found or error loading: {e}")
        logging.info("Creating initial dataset...")
        lfb_dataset.write.mode("overwrite").parquet(output_dataset_path)
        logging.info("Initial dataset created successfully")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("LFB Extract").enableHiveSupport().getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/data/lfb-calls.csv"
        ),
        "outputDatasetPath": sys.argv[1] if len(sys.argv) > 1 else "s3a://dwp/staging",
    }

    logging.info(f"Running LFB Extract with config: {config}")

    run(spark, config)

import logging
import sys
from pyspark.sql import SparkSession

ID_COL = "date"

logging.basicConfig(level=logging.INFO)


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_dataset_path = config["outputDatasetPath"]

    logging.info("Reading new weather dataset...")
    weather_dataset = spark.read.csv(
        input_dataset_path, header=True, inferSchema=True
    ).cache()
    logging.info(f"Dataset loaded with {weather_dataset.count()} rows")

    try:
        logging.info("Attempting to load existing dataset...")
        existing_dataset = spark.read.parquet(output_dataset_path).cache()
        logging.info(f"Existing dataset loaded with {existing_dataset.count()} rows")

        existing_ids = existing_dataset.select(ID_COL).distinct()
        new_rows = weather_dataset.join(existing_ids, ID_COL, "left_anti")
        logging.info(f"Found {new_rows.count()} new rows to add")

        if new_rows.count() > 0:
            logging.info("Appending new rows to existing dataset...")
            new_rows.write.mode("append").parquet(output_dataset_path)
            logging.info(f"Successfully appended {new_rows.count()} new rows")
        else:
            logging.info("No new rows to add, dataset unchanged")

    except Exception as e:
        logging.info(f"No existing dataset found or error loading: {e}")
        logging.info("Creating initial dataset...")
        weather_dataset.write.mode("overwrite").parquet(output_dataset_path)
        logging.info("Initial dataset created successfully")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Weather Extract")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/data/weather.csv"
    output_dataset_path = sys.argv[1] if len(sys.argv) > 1 else "s3a://dwp/staging/weather.parquet"

    config = {
        "inputDatasetPath": input_dataset_path,
        "outputDatasetPath": output_dataset_path,
    }

    run(spark, config)

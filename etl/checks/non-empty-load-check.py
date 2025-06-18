import sys
import logging
from pyspark.sql import SparkSession


DEFAULT_TABLES_STRING = (
    "AirQuality,Date,lfb_call,incident_types,location_type,ward,well_being,weather"
)


def run(spark: SparkSession, config: dict) -> None:
    tables_to_check = config["tablesToCheck"]
    
    for table in tables_to_check:
        logging.info(f"Checking whether {table} exists...")
        table_df = spark.table(table)
        table_row_count = table_df.count()
        assert table_row_count > 0, f"Table {table} is empty"
        logging.info(f"{table} exists and is non-empty")
        logging.info(f"{table} contains {table_row_count} rows")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder.appName("Check for Non-Empty Dimensions and Facts")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    tables_to_check = sys.argv[0] if len(sys.argv) > 0 else DEFAULT_TABLES_STRING

    config = {
        "tablesToCheck": tables_to_check.split(","),
    }

    run(spark, config)

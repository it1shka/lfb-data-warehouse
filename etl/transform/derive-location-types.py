import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import sha2, concat_ws, coalesce, lit

logging.basicConfig(level=logging.INFO)


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_dataset_path = config["outputDatasetPath"]

    logging.info("Reading lfb dataset...")
    lfb_df = spark.read.parquet(input_dataset_path).cache()
    logging.info(f"Read {lfb_df.count()} rows from {input_dataset_path}")

    data = (
        lfb_df.select(["PropertyCategory", "PropertyType"])
        .distinct()
        .withColumn("PropertyCategory", coalesce("PropertyCategory", lit("Unknown")))
        .withColumn("PropertyType", coalesce("PropertyType", lit("Unknown")))
        .withColumn(
            "LocationTypeKey",
            sha2(concat_ws("|", "PropertyCategory", "PropertyType"), 256),
        )
    )

    # Add a sentinel row for unknown location types
    unknown_location_type = spark.createDataFrame(
        [("Unknown", "Unknown", "Unknown")],
        ["PropertyCategory", "PropertyType", "LocationTypeKey"],
    )
    data = data.union(unknown_location_type).cache()

    data.write.mode("overwrite").parquet(output_dataset_path)

    logging.info(f"Wrote {data.count()} rows to {output_dataset_path}")
    data.show(10)
    data.printSchema()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Load The Location Type Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0]
            if len(sys.argv) > 0
            else "s3a://dwp/staging/lfb-calls-clean.parquet"
        ),
        "outputDatasetPath": (
            sys.argv[1]
            if len(sys.argv) > 1
            else "s3a://dwp/staging/location-types.parquet"
        ),
    }

    logging.info(f"Running Load The Location Type Dimension with config: {config}")

    run(spark, config)

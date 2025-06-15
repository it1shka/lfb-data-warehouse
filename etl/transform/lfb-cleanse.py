import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col


COLUMNS_TO_DROP = [
    "CalYear",
    "HourOfCall",
    "AddressQualifier",
    "Postcode_district",
    "UPRN",
    "IncGeo_BoroughName",
    "IncGeo_WardNameNew",
    "Easting_m",
    "Northing_m",
    "Easting_rounded",
    "Northing_rounded",
    "FRS",
    "PumpCount",
]


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    temp_dataset_path = config["tempDatasetPath"]
    output_dataset_path = config["outputDatasetPath"]

    df = spark.read.option("header", "true").load(input_dataset_path)

    # Dropping unnecessary columns and removing "NULL"s
    df = df.drop(*COLUMNS_TO_DROP)
    df = df.replace(to_replace="NULL", value=None)

    # Inferring the schema after the change
    df.write.mode("overwrite").option("header", "true").csv(temp_dataset_path)
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(temp_dataset_path)
    )

    # USRN cannot be zero
    df = df.withColumn("USRN", when(col("USRN") == 0, None).otherwise(col("USRN")))

    # Latitude of an event in London/Britain cannot be 0
    # Longitude, however, can be 0
    # therefore, Longitude is a fake zero
    # only when there is a fake zero in Latitude
    df = df.withColumn(
        "Longitude", when(col("Latitude") == 0, None).otherwise(col("Longitude"))
    )
    df = df.withColumn(
        "Latitude", when(col("Latitude") == 0, None).otherwise(col("Latitude"))
    )

    # Write the result of preprocessing as parquet
    df.write.mode("overwrite").parquet(output_dataset_path)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("LFB Cleanse").enableHiveSupport().getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/staging/lfb-calls.parquet"
    )
    output_dataset_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "s3a://dwp/staging/lfb-calls-clean.parquet"
    )
    temp_dataset_path = (
        sys.argv[2] if len(sys.argv) > 2 else "s3a://dwp/staging/lfb-calls-temp.parquet"
    )

    config = {
        "inputDatasetPath": input_dataset_path,
        "tempDatasetPath": temp_dataset_path,
        "outputDatasetPath": output_dataset_path,
    }

    run(spark, config)

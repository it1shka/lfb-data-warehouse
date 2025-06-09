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
    df = spark.read.option("header", "true").csv(config["lfb_dataset_path"])

    # Dropping unnecessary columns and removing "NULL"s
    df = df.drop(*COLUMNS_TO_DROP)
    df = df.replace(to_replace="NULL", value=None)

    # Inferring the schema after the change
    df.write.mode("overwrite").option("header", "true").csv(config["temp_csv_path"])
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(config["temp_csv_path"])
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
    df.write.mode("overwrite").parquet(config["output_parquet_path"])

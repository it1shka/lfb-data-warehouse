import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import first, row_number


COLUMNS_TO_SELECT = [
    "IncGeo_BoroughCode",
    "ProperCase",
    "IncGeo_WardCode",
    "IncGeo_WardName",
]

RENAMING_STRATEGY = {
    "IncGeo_BoroughCode": "BoroughCode",
    "ProperCase": "BoroughName",
    "IncGeo_WardCode": "WardCode",
    "IncGeo_WardName": "WardName",
}


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_dataset_path = config["outputDatasetPath"]

    df = spark.read.load(input_dataset_path)

    df = df.select(*COLUMNS_TO_SELECT)
    df = df.withColumnsRenamed(RENAMING_STRATEGY)

    df = df.groupBy("WardCode").agg(
        first("WardName").alias("WardName"),
        first("BoroughName").alias("BoroughName"),
        first("BoroughCode").alias("BoroughCode"),
    )

    df = df.withColumn("WardID", row_number())

    df.write.mode("overwrite").parquet(output_dataset_path)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Preparing Ward Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0]
        if len(sys.argv) > 0
        else "s3a://dwp/staging/lfb-calls-clean.parquet"
    )
    output_dataset_path = (
        sys.argv[1] if len(sys.argv) > 1 else "s3a://dwp/staging/ward-dimension.parquet"
    )

    config = {
        "inputDatasetPath": input_dataset_path,
        "outputDatasetPath": output_dataset_path,
    }

    run(spark, config)

from pyspark.sql import SparkSession


COLUMNS_TO_DROP = ["Old Ward Code", "Ward", "Borough"]


COLUMN_RENAMING_STRATEGY = {"New ward code": "Ward_Code"}


def run(spark: SparkSession, config: dict) -> None:
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(config["wb_dataset_path"])
    )

    # Dropping unnecessary columns
    df = df.drop(*COLUMNS_TO_DROP)

    # Renaming problematic columns
    df = df.withColumnsRenamed(COLUMN_RENAMING_STRATEGY)

    # Writing output to parquet
    df.show(10)
    df.write.mode("overwrite").parquet(config["output_parquet_path"])

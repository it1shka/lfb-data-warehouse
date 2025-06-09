from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, round as sparkRound, to_timestamp


COLUMNS_TO_SELECT = [
    "Species",
    "ReadingDateTime",
    "Value",
    # TODO: maybe turn it into a description column
    # "Units"
]


# Amount of floating digits
# allowed for Value column
VALUE_PRECISION = 1


def run(spark: SparkSession, config: dict) -> None:
    df = (
        spark.read.option("recursiveFileLookup", "true")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(config["aq_dataset_folder_path"])
    )

    # Ensuring only needed columns are present
    df = df.select(*COLUMNS_TO_SELECT)

    # Spark detects ReadingDateTime as string,
    # therefore we need to parse it as timestamp
    df = df.withColumn(
        "ReadingDateTime", to_timestamp(col("ReadingDateTime"), "dd/MM/yyyy HH:mm")
    )

    # Grouping by reading time, creating columns for each species, aggregating
    df = (
        df.groupBy("ReadingDateTime")
        .pivot("Species")
        .agg(sparkRound(avg("Value"), VALUE_PRECISION))
    )

    # Saving as parquet
    df.show(10)
    df.write.mode("overwrite").parquet(config["output_parquet_path"])

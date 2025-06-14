import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    avg,
    mean,
    round as sparkRound,
    stddev,
    to_timestamp,
    when,
)


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


# Threshold for detecting outliers
# 3.0 is the most common value
Z_SCORE_THRESHOLD = 3.0


# The Spark job will remove certain
# high values from those pivoted columns
SPECIES_WITH_OUTLIERS = ["CO", "NO", "NO2", "NOX", "O3", "PM1", "PM10", "`PM2.5`"]


# Certain columns with problematic names should be renamed
COLUMN_RENAMING_STRATEGY = [("`PM2.5`", "PM2_5")]


def remove_outliers(
    df: DataFrame, column_name: str, threshold: float = 3.0
) -> DataFrame:
    """Removes outliers (very large values) based on statistical z-score method"""
    statistics = df.select(
        mean(column_name).alias("mean"), stddev(column_name).alias("stddev")
    ).collect()[0]
    mean_val, stddev_val = statistics["mean"], statistics["stddev"]
    scored_df = df.withColumn("__z_score", (col(column_name) - mean_val) / stddev_val)
    clean_df = scored_df.withColumn(
        column_name,
        when(col("__z_score") > threshold, None).otherwise(col(column_name)),
    )
    return clean_df.drop("__z_score")


def run(spark: SparkSession, config: dict) -> None:
    df = (
        spark.read.option("recursiveFileLookup", "true")
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(config["aq_dataset_folder_path"])
    )

    # Ensuring only needed columns are present
    df = df.select(*COLUMNS_TO_SELECT)

    # Value cannot be negative
    # since we measure amount of particles
    df = df.withColumn("Value", when(col("Value") < 0, None).otherwise(col("Value")))

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

    # Removing outliers - anomalies related to very large values
    for species_column in SPECIES_WITH_OUTLIERS:
        df = remove_outliers(df, species_column, Z_SCORE_THRESHOLD)

    # Renaming problematic columns
    for original_name, new_name in COLUMN_RENAMING_STRATEGY:
        df = df.withColumnRenamed(original_name, new_name)

    # Saving as parquet
    df.show(10)
    df.write.mode("overwrite").parquet(config["output_parquet_path"])


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("Air Quality Cleanse")
        .enableHiveSupport()
        .getOrCreate()
    )
    
    logging.info(f"Running with args: {sys.argv}")

    # TODO: change variables
    config = {
        "aq_dataset_folder_path": None,
        "output_parquet_path": None,
    }

    run(spark, config)

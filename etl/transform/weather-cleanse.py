import sys
import logging
from pyspark.sql import Column, SparkSession, DataFrame
from pyspark.sql.functions import col, when


COLUMNS_TO_SELECT = ["date", "tavg", "tmin", "tmax", "wdir", "wspd", "wpgt", "pres"]


# Every tuple has a format of:
# [range start (inclusive), range end (exclusive), replacement label]
BucketingStrategy = list[tuple[float, float, str]]


# Temperature varies from -5.9 to 35.5 in one of the weather csv files.
# According to Google, min London temp is -16.1,
# max London temp is 40.2 (both are historical records)
TEMPERATURE_BUCKETING_STRATEGY = [
    (float("-inf"), -10.0, "Extreme Cold"),
    (-10.0, 0.0, "Very Cold"),
    (0.0, 10.0, "Cold"),
    (10.0, 20.0, "Cool"),
    (20.0, 25.0, "Mild"),
    (25.0, 30.0, "Warm"),
    (30.0, 35.0, "Hot"),
    (35.0, float("inf"), "Very Hot"),
]


TEMPERATURE_COLUMNS = ["tavg", "tmin", "tmax"]


WIND_DIRECTION_BUCKETING_STRATEGY = [
    (337.5, 360.0, "North"),
    (0.0, 22.5, "North"),
    (22.5, 67.5, "Northeast"),
    (67.5, 112.5, "East"),
    (112.5, 157.5, "Southeast"),
    (157.5, 202.5, "South"),
    (202.5, 247.5, "Southwest"),
    (247.5, 292.5, "West"),
    (292.5, 337.5, "Northwest"),
]


# According to Google, max wind gust was 87 km/h
# but in the dataset the value can be up to 100 hm/h
WIND_SPEED_BUCKETING_STRATEGY = [
    (0.0, 1.0, "Calm"),
    (1.0, 5.0, "Light Air"),
    (5.0, 11.0, "Light Breeze"),
    (11.0, 19.0, "Gentle Breeze"),
    (19.0, 28.0, "Moderate Breeze"),
    (28.0, 38.0, "Fresh Breeze"),
    (38.0, 49.0, "Strong Breeze"),
    (49.0, 61.0, "High Wind"),
    (61.0, 74.0, "Gale"),
    (74.0, 88.0, "Severe Gale"),
    (88.0, float("inf"), "Storm"),
]


WIND_SPEED_COLUMNS = ["wspd", "wpgt"]


PRESSURE_BUCKETING_STRATEGY = [
    (float("-inf"), 980, "Very Low Pressure"),
    (980, 1000, "Low Pressure"),
    (1000, 1020, "Normal Pressure"),
    (1020, 1040, "High Pressure"),
    (1040, float("inf"), "Very High Pressure"),
]


def perform_bucketing(
    df: DataFrame, column_name: str, strategy: BucketingStrategy
) -> DataFrame:
    """Maps range of values to a nominal label"""
    aggregated_when: Column | None = None
    for range_start_inc, range_end_exc, label in strategy:
        if aggregated_when is None:
            aggregated_when = when(
                (col(column_name) >= range_start_inc)
                & (col(column_name) < range_end_exc),
                label,
            )
        else:
            aggregated_when = aggregated_when.when(
                (col(column_name) >= range_start_inc)
                & (col(column_name) < range_end_exc),
                label,
            )
    if aggregated_when is None:
        return df
    aggregated_when = aggregated_when.otherwise(None)
    output_df = df.withColumn(column_name, aggregated_when)
    return output_df


def run(spark: SparkSession, config: dict) -> None:
    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(config["weather_dataset_path"])
    )

    # Select columns of interest
    # since there is a chance of downloading
    # dataset with a wrong set of columns.
    # Acts as an additional check
    df = df.select(*COLUMNS_TO_SELECT)

    # Group together temperature values
    # and assign labels to them
    for temp_column in TEMPERATURE_COLUMNS:
        df = perform_bucketing(df, temp_column, TEMPERATURE_BUCKETING_STRATEGY)

    # Just another safety guard
    # ensuring that direction is in range [0, 360)
    df = df.withColumn("wdir", col("wdir") % 360)

    # Replacing angles with geographical direction labels
    df = perform_bucketing(df, "wdir", WIND_DIRECTION_BUCKETING_STRATEGY)

    # Turning wind speed into labels
    for wind_column in WIND_SPEED_COLUMNS:
        df = perform_bucketing(df, wind_column, WIND_SPEED_BUCKETING_STRATEGY)

    # Turning pressure into labels
    df = perform_bucketing(df, "pres", PRESSURE_BUCKETING_STRATEGY)

    # Write output as a parquet file
    df.show(10)
    df.write.mode("overwrite").parquet(config["output_parquet_path"])


if __name__ == '__main__':
    spark = (
        SparkSession.builder.appName("Weather Cleanse")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    # TODO: change variables
    config = {
        "weather_dataset_path": None,
        "output_parquet_path": None,
    }

    run(spark, config)

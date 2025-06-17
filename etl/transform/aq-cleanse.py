import sys
import logging
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import (
    col,
    avg,
    mean,
    round as sparkRound,
    stddev,
    to_timestamp,
    when,
    concat_ws,
    sha2,
    lit,
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
SPECIES_WITH_OUTLIERS = ["CO", "NO", "NO2", "NOX", "O3", "PM1", "PM10", "PM2.5"]


# Certain columns with problematic names should be renamed
COLUMN_RENAMING_STRATEGY = [("PM2.5", "PM2_5")]


# Air Quality Index bucketing strategies
PM10_LEVEL_STRATEGY = [
    (0.0, 25.0, "Good"),
    (25.0, 50.0, "Moderate"),
    (50.0, 75.0, "Unhealthy for Sensitive"),
    (75.0, 100.0, "Unhealthy"),
    (100.0, float("inf"), "Very Unhealthy"),
]

PM2_5_LEVEL_STRATEGY = [
    (0.0, 15.0, "Good"),
    (15.0, 25.0, "Moderate"),
    (25.0, 40.0, "Unhealthy for Sensitive"),
    (40.0, 65.0, "Unhealthy"),
    (65.0, float("inf"), "Very Unhealthy"),
]

PM1_LEVEL_STRATEGY = [
    (0.0, 10.0, "Good"),
    (10.0, 20.0, "Moderate"),
    (20.0, 35.0, "Unhealthy for Sensitive"),
    (35.0, 50.0, "Unhealthy"),
    (50.0, float("inf"), "Very Unhealthy"),
]

NO2_LEVEL_STRATEGY = [
    (0.0, 40.0, "Good"),
    (40.0, 80.0, "Moderate"),
    (80.0, 120.0, "Unhealthy for Sensitive"),
    (120.0, 200.0, "Unhealthy"),
    (200.0, float("inf"), "Very Unhealthy"),
]

NO_LEVEL_STRATEGY = [
    (0.0, 50.0, "Good"),
    (50.0, 100.0, "Moderate"),
    (100.0, 200.0, "Unhealthy for Sensitive"),
    (200.0, 400.0, "Unhealthy"),
    (400.0, float("inf"), "Very Unhealthy"),
]

NOX_LEVEL_STRATEGY = [
    (0.0, 100.0, "Good"),
    (100.0, 200.0, "Moderate"),
    (200.0, 300.0, "Unhealthy for Sensitive"),
    (300.0, 500.0, "Unhealthy"),
    (500.0, float("inf"), "Very Unhealthy"),
]

O3_LEVEL_STRATEGY = [
    (0.0, 100.0, "Good"),
    (100.0, 120.0, "Moderate"),
    (120.0, 180.0, "Unhealthy for Sensitive"),
    (180.0, 240.0, "Unhealthy"),
    (240.0, float("inf"), "Very Unhealthy"),
]

CO_LEVEL_STRATEGY = [
    (0.0, 10.0, "Good"),
    (10.0, 20.0, "Moderate"),
    (20.0, 30.0, "Unhealthy for Sensitive"),
    (30.0, 40.0, "Unhealthy"),
    (40.0, float("inf"), "Very Unhealthy"),
]

AIR_QUALITY_BUCKETING_STRATEGIES = {
    "PM10": PM10_LEVEL_STRATEGY,
    "PM2_5": PM2_5_LEVEL_STRATEGY,
    "PM1": PM1_LEVEL_STRATEGY,
    "NO2": NO2_LEVEL_STRATEGY,
    "NO": NO_LEVEL_STRATEGY,
    "NOX": NOX_LEVEL_STRATEGY,
    "O3": O3_LEVEL_STRATEGY,
    "CO": CO_LEVEL_STRATEGY,
}


def remove_outliers(
    df: DataFrame, column_name: str, threshold: float = 3.0
) -> DataFrame:
    """Removes outliers (very large values) based on statistical z-score method"""
    # Handle case where column doesn't exist or has no non-null values
    if column_name not in df.columns:
        return df

    # Filter out null values for statistics calculation
    non_null_df = df.filter(col(column_name).isNotNull())
    if non_null_df.count() == 0:
        return df

    statistics = non_null_df.select(
        mean(column_name).alias("mean"), stddev(column_name).alias("stddev")
    ).collect()[0]
    mean_val, stddev_val = statistics["mean"], statistics["stddev"]

    # Handle case where standard deviation is 0 or null
    if stddev_val is None or stddev_val == 0:
        return df

    scored_df = df.withColumn(
        "__z_score",
        when(col(column_name).isNull(), None).otherwise(
            (col(column_name) - mean_val) / stddev_val
        ),
    )
    clean_df = scored_df.withColumn(
        column_name,
        when(col("__z_score").isNull(), col(column_name))
        .when(col("__z_score") > threshold, None)
        .otherwise(col(column_name)),
    )
    return clean_df.drop("__z_score")


def perform_bucketing(df: DataFrame, column_name: str, strategy) -> DataFrame:
    aggregated_when: Column | None = None
    for range_start_inc, range_end_exc, label in strategy:
        if aggregated_when is None:
            aggregated_when = when(col(column_name).isNull(), "Unknown").when(
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
    aggregated_when = aggregated_when.otherwise("Unknown")
    output_df = df.withColumn(f"{column_name}Level", aggregated_when)
    return output_df


def apply_air_quality_bucketing(df: DataFrame) -> DataFrame:
    for column_name, strategy in AIR_QUALITY_BUCKETING_STRATEGIES.items():
        # Check if column exists in dataframe
        if column_name in df.columns:
            df = perform_bucketing(df, column_name, strategy)
    return df


def run(spark: SparkSession, config: dict) -> None:
    input_path = config["inputDatasetPath"]
    output_path = config["outputDatasetPath"]

    df = (
        spark.read.option("recursiveFileLookup", "true")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(input_path)
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

    # Renaming problematic columns first to avoid issues with column names containing dots
    for original_name, new_name in COLUMN_RENAMING_STRATEGY:
        df = df.withColumnRenamed(original_name, new_name)

    # Update SPECIES_WITH_OUTLIERS to use renamed column names
    updated_species_with_outliers = []
    for species in SPECIES_WITH_OUTLIERS:
        # Check if this species was renamed
        renamed = False
        for original_name, new_name in COLUMN_RENAMING_STRATEGY:
            if species == original_name:
                updated_species_with_outliers.append(new_name)
                renamed = True
                break
        if not renamed:
            updated_species_with_outliers.append(species)

    # Removing outliers - anomalies related to very large values
    for species_column in updated_species_with_outliers:
        df = remove_outliers(df, species_column, Z_SCORE_THRESHOLD)

    # Apply air quality bucketing to convert numerical values to health categories
    df = apply_air_quality_bucketing(df)

    # Drop original columns that now have corresponding Level columns
    columns_to_drop = []
    for column_name in AIR_QUALITY_BUCKETING_STRATEGIES.keys():
        if column_name in df.columns and f"{column_name}Level" in df.columns:
            columns_to_drop.append(column_name)

    if columns_to_drop:
        df = df.drop(*columns_to_drop)
        print(f"Dropped original columns: {columns_to_drop}")

    # Add hash key containing all columns except ReadingDateTime
    non_date_columns = [
        col_name for col_name in df.columns if col_name != "ReadingDateTime"
    ]

    # Create hash key by concatenating all non-date columns and hashing them
    if non_date_columns:
        df = df.withColumn(
            "AirQualityKey",
            sha2(concat_ws("|", *[col(c) for c in non_date_columns]), 256),
        )

    # Add sentinel row with all nulls for handling null foreign keys in fact tables
    sentinel_values = {}
    for column in df.columns:
        if column == "ReadingDateTime":
            sentinel_values[column] = lit(None).cast("timestamp")
        elif column == "AirQualityKey":
            sentinel_values[column] = lit("Unknown")
        else:
            sentinel_values[column] = lit("Unknown").cast("string")

    # Create sentinel row dataframe
    sentinel_df = spark.createDataFrame([()]).select(
        *[sentinel_values[col_name].alias(col_name) for col_name in df.columns]
    )

    # Union the main dataframe with the sentinel row
    df = df.union(sentinel_df)

    # Show sample of processed data
    print("Sample of processed air quality data:")
    df.show(10)

    # Show schema of final output
    print("Final schema:")
    df.printSchema()

    # Saving as parquet
    df.write.mode("overwrite").parquet(output_path)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Air Quality Cleanse")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/staging/air-quality.parquet"
    )
    output_dataset_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "s3a://dwp/staging/air-quality-clean.parquet"
    )

    config = {
        "inputDatasetPath": input_dataset_path,
        "outputDatasetPath": output_dataset_path,
    }

    run(spark, config)

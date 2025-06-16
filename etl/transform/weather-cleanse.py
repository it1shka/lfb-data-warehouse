import sys
import logging
from pyspark.sql import Column, SparkSession, DataFrame
from pyspark.sql.functions import col, when, abs as spark_abs


COLUMNS_TO_SELECT = ["date", "tavg", "tmin", "tmax", "wdir", "wspd", "wpgt", "pres", "prcp", "snow", "tsun"]


# Temperature bucketing for average temperature (°C)
TEMPERATURE_CATEGORY_STRATEGY = [
    (float("-inf"), -10.0, "Very Cold"),
    (-10.0, 0.0, "Cold"), 
    (0.0, 10.0, "Cool"),
    (10.0, 20.0, "Mild"),
    (20.0, 25.0, "Warm"),
    (25.0, 30.0, "Hot"),
    (30.0, float("inf"), "Very Hot"),
]

# Temperature amplitude bucketing
TEMPERATURE_AMPLITUDE_STRATEGY = [
    (0.0, 5.0, "Low"),
    (5.0, 10.0, "Moderate"),
    (10.0, 15.0, "High"),
    (15.0, float("inf"), "Very High"),
]

# Wind direction bucketing (degrees)
WIND_DIRECTION_STRATEGY = [
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

# Wind strength bucketing (km/h) - based on Beaufort scale
WIND_STRENGTH_STRATEGY = [
    (0.0, 1.0, "Calm"),
    (1.0, 5.0, "Light Air"),
    (5.0, 11.0, "Light Breeze"),
    (11.0, 19.0, "Gentle Breeze"),
    (19.0, 28.0, "Moderate Breeze"),
    (28.0, 38.0, "Fresh Breeze"),
    (38.0, 49.0, "Strong Breeze"),
    (49.0, 61.0, "Moderate Gale"),
    (61.0, 74.0, "Gale"),
    (74.0, 88.0, "Severe Gale"),
    (88.0, 102.0, "Storm"),
    (102.0, 117.0, "Violent Storm"),
    (117.0, float("inf"), "Hurricane"),
]

# Precipitation bucketing (mm)
PRECIPITATION_LEVEL_STRATEGY = [
    (0.0, 0.1, "None"),
    (0.1, 2.5, "Light"),
    (2.5, 10.0, "Moderate"),
    (10.0, 25.0, "Heavy"),
    (25.0, float("inf"), "Very Heavy"),
]

# Snow bucketing (mm)
SNOW_LEVEL_STRATEGY = [
    (0.0, 0.1, "None"),
    (0.1, 5.0, "Light"),
    (5.0, 15.0, "Moderate"),
    (15.0, 30.0, "Heavy"),
    (30.0, float("inf"), "Very Heavy"),
]

# Wind gustiness bucketing (percentage increase from sustained wind to gust)
WIND_GUSTINESS_STRATEGY = [
    (0.0, 20.0, "Low"),
    (20.0, 50.0, "Moderate"),
    (50.0, 100.0, "High"),
    (100.0, float("inf"), "Very High"),
]

# Atmospheric pressure bucketing (hPa)
PRESSURE_LEVEL_STRATEGY = [
    (float("-inf"), 980.0, "Very Low"),
    (980.0, 1000.0, "Low"),
    (1000.0, 1020.0, "Normal"),
    (1020.0, 1040.0, "High"),
    (1040.0, float("inf"), "Very High"),
]

# Sunshine duration bucketing (seconds - tsun represents time difference between sunrise and sunset)
# Assuming values around 28000-30000 seconds (~8-8.5 hours typical for winter/summer variation)
SUNSHINE_LEVEL_STRATEGY = [
    (0.0, 25200.0, "Very Short Day"),      # < 7 hours
    (25200.0, 28800.0, "Short Day"),       # 7-8 hours  
    (28800.0, 32400.0, "Normal Day"),  # 8-9 hours
    (32400.0, 36000.0, "Long Day"),      # 9-10 hours
    (36000.0, float("inf"), "Very Long Day"), # > 10 hours
]


def perform_bucketing(
    df: DataFrame, column_name: str, strategy
) -> DataFrame:
    """Maps range of values to a nominal label, handling NULL values as 'Unknown'"""
    aggregated_when: Column | None = None
    for range_start_inc, range_end_exc, label in strategy:
        if aggregated_when is None:
            aggregated_when = when(
                col(column_name).isNull(),
                "Unknown"
            ).when(
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
    output_df = df.withColumn(column_name, aggregated_when)
    return output_df


def calculate_derived_columns(df: DataFrame) -> DataFrame:
    """Calculate derived columns for better weather analysis"""
    
    # Temperature amplitude (difference between max and min temperature)
    # Handle NULL values by setting amplitude to NULL if either tmin or tmax is NULL
    df = df.withColumn(
        "TemperatureAmplitude", 
        when(col("tmin").isNull() | col("tmax").isNull(), None)
        .otherwise(col("tmax") - col("tmin"))
    )
    
    # Wind gustiness (percentage increase from sustained wind to peak gust)
    # Handle division by zero and NULL values
    df = df.withColumn(
        "WindGustinessPct",
        when(col("wspd").isNull() | col("wpgt").isNull(), None)
        .when(col("wspd") == 0, 0.0)
        .otherwise((col("wpgt") - col("wspd")) / col("wspd") * 100)
    )
    
    return df


def apply_all_bucketing(df: DataFrame) -> DataFrame:
    """Apply bucketing strategies to all relevant columns"""
    
    # Temperature category (based on average temperature)
    df = perform_bucketing(df, "tavg", TEMPERATURE_CATEGORY_STRATEGY)
    df = df.withColumnRenamed("tavg", "TemperatureCategory")
    
    # Temperature amplitude bucketing
    df = perform_bucketing(df, "TemperatureAmplitude", TEMPERATURE_AMPLITUDE_STRATEGY)
    
    # Wind direction bucketing (ensure direction is in range [0, 360) and handle NULLs)
    df = df.withColumn(
        "wdir", 
        when(col("wdir").isNull(), None)
        .otherwise(col("wdir") % 360)
    )
    df = perform_bucketing(df, "wdir", WIND_DIRECTION_STRATEGY)
    df = df.withColumnRenamed("wdir", "WindDirection")
    
    # Wind strength bucketing (based on sustained wind speed)
    df = perform_bucketing(df, "wspd", WIND_STRENGTH_STRATEGY)
    df = df.withColumnRenamed("wspd", "WindStrength")

    # Wind gustiness bucketing
    df = perform_bucketing(df, "WindGustinessPct", WIND_GUSTINESS_STRATEGY)
    df = df.withColumnRenamed("WindGustinessPct", "WindGustiness")
    
    # Atmospheric pressure bucketing
    df = perform_bucketing(df, "pres", PRESSURE_LEVEL_STRATEGY)
    df = df.withColumnRenamed("pres", "PressureLevel")

    # Precipitation bucketing
    df = perform_bucketing(df, "prcp", PRECIPITATION_LEVEL_STRATEGY)
    df = df.withColumnRenamed("prcp", "PrecipitationLevel")

    # Snow bucketing
    df = perform_bucketing(df, "snow", SNOW_LEVEL_STRATEGY)
    df = df.withColumnRenamed("snow", "SnowLevel")

    # Sunshine duration bucketing
    df = perform_bucketing(df, "tsun", SUNSHINE_LEVEL_STRATEGY)
    df = df.withColumnRenamed("tsun", "SunshineLevel")

    # Drop intermediate columns that are no longer needed
    df = df.drop("tmin", "tmax", "wpgt")

    return df


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_dataset_path = config["outputDatasetPath"]

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .load(input_dataset_path)
    )

    # Select columns of interest
    # since there is a chance of downloading
    # dataset with a wrong set of columns.
    # Acts as an additional check
    df = df.select(*COLUMNS_TO_SELECT)

    # Calculate derived columns (temperature amplitude, wind gustiness)
    df = calculate_derived_columns(df)

    # Apply all bucketing strategies to convert numerical values to descriptive categories
    df = apply_all_bucketing(df)

    # Show sample of processed data
    print("Sample of processed weather data:")
    df.show(10)
    
    # Show schema of final output
    print("Final schema:")
    df.printSchema()

    # Write output as a parquet file
    df.write.mode("overwrite").parquet(output_dataset_path)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Weather Cleanse")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/staging/weather.parquet"
    )
    output_dataset_path = (
        sys.argv[1] if len(sys.argv) > 1 else "s3a://dwp/staging/weather-clean.parquet"
    )

    config = {
        "inputDatasetPath": input_dataset_path,
        "outputDatasetPath": output_dataset_path,
    }

    run(spark, config)

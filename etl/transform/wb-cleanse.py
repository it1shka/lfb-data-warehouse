import sys
import logging
from pyspark.sql import Column, SparkSession, DataFrame
from pyspark.sql.functions import col, when

COLUMNS_TO_DROP = ["Old Ward Code", "Ward", "Borough"]

COLUMN_RENAMING_STRATEGY = {"New ward code": "Ward_Code"}

BucketingStrategy = list[tuple[float, float, str]]

LIFE_EXPECTANCY_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 78.0, "Very Low"),
    (78.0, 80.0, "Low"),
    (80.0, 82.0, "Medium"),
    (82.0, 85.0, "High"),
    (85.0, float("inf"), "Very High"),
]

CHILDHOOD_OBESITY_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 12.0, "Very Low"),
    (12.0, 18.0, "Low"),
    (18.0, 24.0, "Medium"),
    (24.0, 30.0, "High"),
    (30.0, float("inf"), "Very High"),
]

INCAPACITY_BENEFIT_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 1.5, "Very Low"),
    (1.5, 3.0, "Low"),
    (3.0, 5.0, "Medium"),
    (5.0, 8.0, "High"),
    (8.0, float("inf"), "Very High"),
]

UNEMPLOYMENT_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 2.0, "Very Low"),
    (2.0, 5.0, "Low"),
    (5.0, 8.0, "Medium"),
    (8.0, 12.0, "High"),
    (12.0, float("inf"), "Very High"),
]

CRIME_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 60.0, "Very Low"),
    (60.0, 90.0, "Low"),
    (90.0, 120.0, "Medium"),
    (120.0, 180.0, "High"),
    (180.0, float("inf"), "Very High"),
]

GCSE_POINTS_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 310.0, "Very Low"),
    (310.0, 330.0, "Low"),
    (330.0, 350.0, "Medium"),
    (350.0, 370.0, "High"),
    (370.0, float("inf"), "Very High"),
]

PUBLIC_TRANSPORT_ACCESS_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 0.0, "Below Average"),
    (0.0, float("inf"), "Above Average"),
]

DELIBERATE_FIRES_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 0.0, "Below Average"),
    (0.0, float("inf"), "Above Average"),
]

UNAUTHORISED_ABSENCE_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 0.6, "Very Low"),
    (0.6, 0.9, "Low"),
    (0.9, 1.2, "Medium"),
    (1.2, 1.6, "High"),
    (1.6, float("inf"), "Very High"),
]

DEPENDENT_CHILDREN_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 10.0, "Very Low"),
    (10.0, 17.0, "Low"),
    (17.0, 27.0, "Medium"),
    (27.0, 40.0, "High"),
    (40.0, float("inf"), "Very High"),
]

HOMES_WITH_ACCESS_BUCKETING_STRATEGY: BucketingStrategy = [
    (float("-inf"), 0.0, "Below Average"),
    (0.0, float("inf"), "Above Average"),
]


# opt in/out certain columns
GLOBAL_BUCKETING_STRATEGY = {
    "Life_Expectancy": LIFE_EXPECTANCY_BUCKETING_STRATEGY,
    "Childhood_Obesity": CHILDHOOD_OBESITY_BUCKETING_STRATEGY,
    "Incapacity_Benefit": INCAPACITY_BENEFIT_BUCKETING_STRATEGY,
    "Unemployment": UNEMPLOYMENT_BUCKETING_STRATEGY,
    "Crime": CRIME_BUCKETING_STRATEGY,
    "GCSE_points": GCSE_POINTS_BUCKETING_STRATEGY,
    "Public_Transport_Access": PUBLIC_TRANSPORT_ACCESS_BUCKETING_STRATEGY,
    "Deliberate_Fires": DELIBERATE_FIRES_BUCKETING_STRATEGY,
    "Unauthorised_Absence": UNAUTHORISED_ABSENCE_BUCKETING_STRATEGY,
    "Dependent_children": DEPENDENT_CHILDREN_BUCKETING_STRATEGY,
    "Homes_with_access": HOMES_WITH_ACCESS_BUCKETING_STRATEGY,
}


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
    output_df = df.withColumn(column_name + "_Bucket", aggregated_when)
    return output_df


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_dataset_path = config["outputDatasetPath"]

    df = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .load(input_dataset_path)
    )

    # Dropping unnecessary columns
    df = df.drop(*COLUMNS_TO_DROP)

    # Renaming problematic columns
    df = df.withColumnsRenamed(COLUMN_RENAMING_STRATEGY)

    # Apply buckets
    for column_name, bucketing_strategy in GLOBAL_BUCKETING_STRATEGY.items():
        df = perform_bucketing(df, column_name, bucketing_strategy)

    # Writing output to parquet
    df.show(10)
    df.write.mode("overwrite").parquet(output_dataset_path)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Well-Being Cleanse")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/staging/well-being.parquet"
    )
    output_dataset_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "s3a://dwp/staging/well-being-clean.parquet"
    )

    config = {
        "inputDatasetPath": input_dataset_path,
        "outputDatasetPath": output_dataset_path,
    }

    run(spark, config)

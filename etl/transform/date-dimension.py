import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    min,
    max,
    col,
    explode,
    sequence,
    year,
    month,
    dayofmonth,
    dayofweek,
    date_format,
    quarter,
    weekofyear,
    when,
    expr,
)


def run(spark: SparkSession, config: dict) -> None:
    lfb_calls_path = config["lfbCallsCleanPath"]
    output_path = config["outputDatasetPath"]

    logging.info("Starting date dimension creation")

    try:
        logging.info(f"Reading LFB calls dataset from {lfb_calls_path}")
        lfb_df = spark.read.parquet(lfb_calls_path)
    except Exception as e:
        logging.error(f"Error reading LFB calls dataset: {e}")
        sys.exit(1)

    logging.info(f"Successfully read LFB calls dataset with {lfb_df.count()} records")
    date_stats = lfb_df.select(
        min(col("dateOfCall")).alias("min_date"),
        max(col("dateOfCall")).alias("max_date"),
    ).collect()[0]
    min_date = date_stats["min_date"]
    max_date = date_stats["max_date"]
    logging.info(f"Detected date range: {min_date} to {max_date}")
    date_range_df = spark.createDataFrame(
        [(min_date, max_date)], ["start_date", "end_date"]
    )
    all_dates_df = date_range_df.select(
        explode(
            sequence(col("start_date"), col("end_date"), expr("interval 1 day"))
        ).alias("date")
    )
    date_dimension_df = all_dates_df.select(
        col("date").alias("Date"),
        # Basic date components
        year(col("date")).alias("Year"),
        month(col("date")).alias("Month"),
        dayofmonth(col("date")).alias("Day"),
        # Day of week (1=Sunday, 7=Saturday in Spark, so we adjust)
        when(dayofweek(col("date")) == 1, 7)  # Sunday becomes 7
        .otherwise(dayofweek(col("date")) - 1)
        .alias("DayOfWeek"),  # Others shift down
        # Day and month names
        date_format(col("date"), "EEEE").alias("DayName"),
        date_format(col("date"), "MMMM").alias("MonthName"),
        # Quarter and week
        quarter(col("date")).alias("Quarter"),
        weekofyear(col("date")).alias("WeekOfYear"),
        # Weekend indicator (Saturday=6, Sunday=7)
        when(
            (dayofweek(col("date")) == 1)  # Sunday
            | (dayofweek(col("date")) == 7),  # Saturday
            True,
        )
        .otherwise(False)
        .alias("IsWeekend"),
    )

    try:
        existing_df = spark.read.parquet(output_path)
        logging.info("Found existing date dimension, filtering out existing dates")

        new_df = date_dimension_df.join(existing_df, "Date", "left_anti")

        new_dates_count = new_df.count()
        if new_dates_count == 0:
            logging.info("No new dates to add")
            return

        logging.info(f"Adding {new_dates_count} new dates to dimension")
        new_df.write.mode("append").parquet(output_path)
        logging.info("New dates added successfully")

    except Exception as e:
        logging.info("No existing date dimension found, creating from scratch")
        date_dimension_df.write.mode("overwrite").parquet(output_path)
        logging.info("Date dimension created successfully")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Create Date Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "lfbCallsCleanPath": (
            sys.argv[0]
            if len(sys.argv) > 0
            else "s3a://dwp/staging/lfb-calls-clean.parquet"
        ),
        "outputDatasetPath": (
            sys.argv[1] if len(sys.argv) > 1 else "s3a://dwp/staging/date.parquet"
        ),
    }

    run(spark, config)

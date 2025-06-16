import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

logging.basicConfig(level=logging.INFO)


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_table_name = config["outputTableName"]

    logging.info("Reading air quality dataset...")
    air_quality_df = spark.read.parquet(input_dataset_path).cache()
    logging.info(f"Air quality dataset loaded with {air_quality_df.count()} rows")

    logging.info("Transforming air quality dataset...")
    air_quality_df_clean = air_quality_df.drop("ReadingDateTime").distinct()

    new_schema = StructType(
        [
            StructField("PM10Level", StringType(), False),
            StructField("PM2_5Level", StringType(), False),
            StructField("PM1Level", StringType(), False),
            StructField("NO2Level", StringType(), False),
            StructField("NOLevel", StringType(), False),
            StructField("NOXLevel", StringType(), False),
            StructField("O3Level", StringType(), False),
            StructField("COLevel", StringType(), False),
            StructField("AirQualityKey", StringType(), False),
        ]
    )
    air_quality_df_clean = spark.createDataFrame(
        air_quality_df_clean.rdd, schema=new_schema
    )

    logging.info("Writing to Delta table with optimizations and partitioning...")
    (
        air_quality_df_clean.write.mode("overwrite")
        .format("delta")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("overwriteSchema", "true")
        .saveAsTable(output_table_name)
    )
    spark.sql(
        f"OPTIMIZE {output_table_name} ZORDER BY (AirQualityKey, PM10Level, PM2_5Level, NO2Level)"
    )

    # Cache the dimension table for faster joins (dimension tables are typically small)
    logging.info("Caching dimension table for faster joins...")
    spark.sql(f"CACHE TABLE {output_table_name}")

    logging.info(
        f"Air quality dimension table '{output_table_name}' created and optimized successfully"
    )
    spark.sql(f"DESCRIBE DETAIL {output_table_name}").show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Load Air Quality Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0]
            if len(sys.argv) >= 1
            else "s3a://dwp/staging/air-quality-clean.parquet"
        ),
        "outputTableName": sys.argv[1] if len(sys.argv) >= 2 else "AirQuality",
    }

    logging.info(f"Running Load Air Quality Dimension with config: {config}")

    run(spark, config)

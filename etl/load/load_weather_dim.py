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

    logging.info("Reading weather dataset...")
    weather_df = spark.read.parquet(input_dataset_path).cache()
    logging.info(f"Weather dataset loaded with {weather_df.count()} rows")

    logging.info("Transforming weather dataset...")
    weather_df_clean = weather_df.drop("date").distinct()

    new_schema = StructType(
        [
            StructField("TemperatureCategory", StringType(), False),
            StructField("WindDirection", StringType(), False),
            StructField("Wind", StringType(), False),
            StructField("Pressure", StringType(), False),
            StructField("Precipitation", StringType(), False),
            StructField("Snow", StringType(), False),
            StructField("Sunshine", StringType(), False),
            StructField("TemperatureAmplitude", StringType(), False),
            StructField("WindGustiness", StringType(), False),
        ]
    )
    weather_df_clean = spark.createDataFrame(weather_df_clean.rdd, schema=new_schema)

    logging.info("Writing to Delta table with optimizations and partitioning...")
    (
        weather_df_clean.write.mode("overwrite")
        .format("delta")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("overwriteSchema", "true")
        .partitionBy("TemperatureCategory")
        .saveAsTable(output_table_name)
    )
    spark.sql(
        f"OPTIMIZE {output_table_name} ZORDER BY (WindDirection)"
    )

    logging.info(
        f"Weather dimension table '{output_table_name}' created and optimized successfully"
    )
    spark.sql(f"DESCRIBE DETAIL {output_table_name}").show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Load Weather Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0]
            if len(sys.argv) > 1
            else "s3a://dwp/staging/weather-clean.parquet"
        ),
        "outputTableName": sys.argv[1] if len(sys.argv) > 2 else "Weather",
    }

    logging.info(f"Running Load Weather Dimension with config: {config}")

    run(spark, config)

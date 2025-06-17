import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)

logging.basicConfig(level=logging.INFO)

SCHEMA = [
    StructField("PropertyCategory", StringType(), nullable=False),
    StructField("PropertyType", StringType(), nullable=False),
    StructField("LocationTypeKey", StringType(), nullable=False),
]


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_table_name = config["outputTableName"]

    logging.info("Reading location types...")
    location_types_df = spark.read.parquet(input_dataset_path).cache()
    logging.info(f"Read {location_types_df.count()} rows from {input_dataset_path}")

    logging.info("Transforming location types...")
    location_types = spark.createDataFrame(
        location_types_df.rdd, schema=StructType(SCHEMA)
    )

    logging.info("Writing to Delta table with optimizations...")
    (
        location_types.write.mode("overwrite")
        .format("delta")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("overwriteSchema", "true")
        .saveAsTable(output_table_name)
    )
    spark.sql(
        f"OPTIMIZE {output_table_name} ZORDER BY (PropertyCategory, PropertyType)"
    )
    logging.info(
        f"Location type dimension table '{output_table_name}' created and optimized successfully"
    )
    spark.sql(f"DESCRIBE DETAIL {output_table_name}").show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Load The Location Type Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0]
            if len(sys.argv) > 0
            else "s3a://dwp/staging/location-types.parquet"
        ),
        "outputTableName": sys.argv[1] if len(sys.argv) > 1 else "location_type",
    }

    logging.info(f"Running Load The Location Type Dimension with config: {config}")

    run(spark, config)

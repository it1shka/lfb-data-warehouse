import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
)
from pyspark.sql.functions import col

logging.basicConfig(level=logging.INFO)


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_table_name = config["outputTableName"]

    logging.info("Reading incident types dataset...")
    incident_types_df = spark.read.parquet(input_dataset_path).cache()
    logging.info(f"Incident types dataset loaded with {incident_types_df.count()} rows")

    logging.info("Transforming incident types dataset...")

    scd_schema = StructType(
        [
            StructField("IncidentType", StringType(), False),
            StructField("IncidentDescription", StringType(), False),
            StructField("IncidentTypeKey", IntegerType(), False),
        ]
    )

    incident_types_clean = spark.createDataFrame(
        incident_types_df.rdd, schema=scd_schema
    )

    logging.info("Writing to Delta table...")
    (
        incident_types_clean.write.mode("overwrite")
        .format("delta")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("overwriteSchema", "true")
        .saveAsTable(output_table_name)
    )

    logging.info("Optimizing table with Z-ordering...")
    spark.sql(f"OPTIMIZE {output_table_name} ZORDER BY (IncidentTypeKey)")
    logging.info("Z-ordering optimization completed")

    logging.info(
        f"Incident Types dimension table '{output_table_name}' created and optimized successfully"
    )
    spark.sql(f"DESCRIBE DETAIL {output_table_name}").show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Incident Types Loader")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0]
            if len(sys.argv) > 0
            else "s3a://dwp/staging/incident-type.parquet"
        ),
        "outputTableName": sys.argv[1] if len(sys.argv) > 1 else "IncidentType",
    }

    logging.info(f"Running Load Incident Types with config: {config}")

    run(spark, config)

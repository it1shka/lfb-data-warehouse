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

SCHEMA = [
    StructField("WardCode", StringType(), nullable=True),
    StructField("WardName", StringType(), nullable=True),
    StructField("BoroughName", StringType(), nullable=True),
    StructField("BoroughCode", StringType(), nullable=True),
    StructField("WardID", StringType(), nullable=True),
]


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_table_name = config["outputTableName"]

    logging.info("Reading ward dataset...")
    ward_df = spark.read.parquet(input_dataset_path).cache()
    logging.info(f"Ward dataset loaded with {ward_df.count()} rows")
    
    logging.info("Transforming ward dataset...")
    ward_dimension = spark.createDataFrame(
        ward_df.rdd, schema=StructType(SCHEMA)
    )

    logging.info("Writing to Delta table with optimizations...")
    (
        ward_dimension.write.mode("overwrite")
        .format("delta")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("overwriteSchema", "true")
        .partitionBy("BoroughCode")
        .saveAsTable(output_table_name)
    )
    spark.sql(
        f"OPTIMIZE {output_table_name} ZORDER BY (WardCode)"
    )
    logging.info(
        f"Ward dimension table '{output_table_name}' created and optimized successfully"
    )
    spark.sql(f"DESCRIBE DETAIL {output_table_name}").show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Ward Dimension Loader")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0]
            if len(sys.argv) > 0
            else "s3a://dwp/staging/ward-dimension.parquet"
        ),
        "outputTableName": sys.argv[1] if len(sys.argv) > 1 else "ward",
    }

    logging.info(f"Running Load Ward Dimension with config: {config}")

    run(spark, config)

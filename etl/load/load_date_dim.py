import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    StringType,
    BooleanType,
)

logging.basicConfig(level=logging.INFO)


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_table_name = config["outputTableName"]

    logging.info("Reading date dataset...")
    date_df = spark.read.parquet(input_dataset_path).cache()
    logging.info(f"Date dataset loaded with {date_df.count()} rows")

    logging.info("Transforming date dataset...")
    new_schema = StructType(
        [
            StructField("Date", DateType(), False),
            StructField("Year", IntegerType(), False),
            StructField("Month", IntegerType(), False),
            StructField("Day", IntegerType(), False),
            StructField("DayOfWeek", IntegerType(), False),
            StructField("DayName", StringType(), False),
            StructField("MonthName", StringType(), False),
            StructField("Quarter", IntegerType(), False),
            StructField("WeekOfYear", IntegerType(), False),
            StructField("IsWeekend", BooleanType(), False),
        ]
    )
    date_df_clean = spark.createDataFrame(date_df.rdd, schema=new_schema)

    logging.info("Writing to Delta table with optimizations and partitioning...")
    (
        date_df_clean.write.mode("overwrite")
        .format("delta")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("overwriteSchema", "true")
        .partitionBy("Year")
        .saveAsTable(output_table_name)
    )
    spark.sql(f"OPTIMIZE {output_table_name} ZORDER BY (Month, Date)")

    logging.info(
        f"Date dimension table '{output_table_name}' created and optimized successfully"
    )
    spark.sql(f"DESCRIBE DETAIL {output_table_name}").show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Load Date Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/staging/date.parquet"
        ),
        "outputTableName": sys.argv[1] if len(sys.argv) > 1 else "Date",
    }

    logging.info(f"Running Load Date Dimension with config: {config}")

    run(spark, config)

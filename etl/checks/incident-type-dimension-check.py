import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing import assertSchemaEqual
import pyspark.sql.functions as F
import pyspark.sql.types as T


def run(spark: SparkSession, config: dict) -> None:
    ...


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder.appName("Checking Incident Type Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/staging/incident-type.parquet"
    )
    schema_check = (
        sys.argv[1] in ["True", "true", "enabled", "yes"] if len(sys.argv) > 1 else True
    )

    config = {
        "inputDatasetPath": input_dataset_path,
        "schemaCheck": schema_check,
    }

    run(spark, config)

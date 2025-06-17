import sys
import logging
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


UNIQUE_COLUMNS = ["WellBeingID"]
NON_NULL_BASE_COLUMNS = ["WellBeingID", "WardCode", "Year"]
NON_NULL_POSTFIX = "Label"


def assert_unique(df: DataFrame, col_name: str) -> None:
    """Asserts that col_name does not contain repetitions"""
    col_count = f"__{col_name}_count"
    agg_df = df.select(col_name).groupBy(col_name).agg(F.count("*").alias(col_count))
    repetitions = agg_df.filter(F.col(col_count) > 1)
    for repetition_row in repetitions.collect():
        repetition_value = repetition_row[col_name]
        repetition_count = repetition_row[col_count]
        logging.error(
            f"Value `{repetition_value}` is repeated {repetition_count} times"
        )
    repetitions_count = repetitions.count()
    assert (
        repetitions_count <= 0
    ), f"Column {col_name} contains {repetitions_count} repetitive values"


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]

    df = spark.read.load(input_dataset_path)

    # asserting non-null for some columns
    for col in df.columns:
        if col not in NON_NULL_BASE_COLUMNS and not col.endswith(NON_NULL_POSTFIX):
            continue
        null_count = df.select(col).filter(F.col(col).isNull()).count()
        assert null_count <= 0, f"Column {col} contains {null_count} nulls"

    # asserting uniqueness
    for col in UNIQUE_COLUMNS:
        assert_unique(df, col)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder.appName("Checking Well-Being Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/staging/well-being-dimension.parquet"
    )

    config = {
"inputDatasetPath": input_dataset_path,
    }

    run(spark, config)

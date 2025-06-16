import sys
import logging
from pyspark.sql import SparkSession


def run(spark: SparkSession, config: dict) -> None:
    dataset_input_path = config["datasetInputPath"]
    expected_column_count = config["expectedColumnCount"]
    primary_key = config["primaryKey"]

    df = spark.read.load(dataset_input_path)
    df.cache()

    # making sure the dataset is not empty
    row_count = df.count()
    logging.info(f"Dataset loaded with {row_count} rows")
    assert row_count > 0, "Dataset is empty"

    # making sure the dataset at least has the expected number of columns
    column_count = len(df.columns)
    logging.info(f"Dataset has {column_count} columns")
    assert (
        column_count == expected_column_count
    ), f"Expected {expected_column_count} columns, found {column_count}"

    # making sure there are no duplicate rows
    if primary_key is not None and len(primary_key) > 0:
        pk_count = df.groupBy(primary_key).count().filter("count > 1").count()
        logging.info(
            f"Found {pk_count} duplicate rows based on primary key '{primary_key}'"
        )
        assert (
            pk_count == 0
        ), f"Dataset contains duplicate rows based on primary key '{primary_key}'"

    # printing completeness of each column in the dataset
    for col in df.columns:
        completeness = df.filter(df[col].isNotNull()).count() / row_count
        logging.info(f"Column '{col}' completeness: {completeness:.2%}")
        assert completeness > 0.0, f"Column '{col}' contains only NULLs"


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder.appName("Post-Extract Check")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    assert (
        len(sys.argv) == 3
    ), "Expected input dataset, column count, primary key (may be empty or None) as arguments"
    dataset_input_path, expected_column_count, primary_key = (
        sys.argv[0],
        int(sys.argv[1]),
        sys.argv[2],
    )

    config = {
        "datasetInputPath": dataset_input_path,
        "expectedColumnCount": expected_column_count,
        "primaryKey": primary_key,
    }

    run(spark, config)

import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing import assertSchemaEqual
import pyspark.sql.functions as F
import pyspark.sql.types as T


EXPECTED_SCHEMA = T.StructType(
    [
        T.StructField("IncidentType", T.StringType()),
        T.StructField("IncidentDescription", T.StringType()),
        T.StructField("IncidentTypeKey", T.IntegerType()),
    ]
)


UNIQUE_COLUMNS = ["IncidentTypeKey"]


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
    schema_check = config["schemaCheck"]

    df = spark.read.load(input_dataset_path)
    df.cache()

    logging.info("Checking Incident Type dimension")
    df.printSchema()

    for col in df.columns:
        null_count = df.select(col).filter(F.col(col).isNull()).count()
        assert null_count <= 0, f"Column {col} contains {null_count} nulls"

    # asserting the schema
    if schema_check:
        assertSchemaEqual(
            actual=df.schema,
            expected=EXPECTED_SCHEMA,
            # ignoreColumnName=False, # Not available in pyspark 3.5.5
            # ignoreColumnOrder=True,
            # ignoreNullable=True,
        )

    # checking uniqueness
    for col in UNIQUE_COLUMNS:
        assert_unique(df, col)


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

import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing import assertSchemaEqual
import pyspark.sql.functions as F
import pyspark.sql.types as T


EXPECTED_SCHEMA = T.StructType(
    [
        T.StructField("Date", T.DateType()),
        T.StructField("Year", T.IntegerType()),
        T.StructField("Month", T.IntegerType()),
        T.StructField("Day", T.IntegerType()),
        T.StructField("DayOfWeek", T.IntegerType()),
        T.StructField("DayName", T.StringType()),
        T.StructField("MonthName", T.StringType()),
        T.StructField("Quarter", T.IntegerType()),
        T.StructField("WeekOfYear", T.IntegerType()),
        T.StructField("IsWeekend", T.BooleanType()),
    ]
)


UNIQUE_COLUMNS = ["Date"]


CHECK_SET_CARDINALITY_STRATEGY = {
    "Month": 12,
    "MonthName": 12,
    "Day": 31,
    "DayOfWeek": 7,
    "DayName": 7,
    "Quarter": 4,
    "IsWeekend": 2,
}

CHECK_RANGE_STRATEGY = {
    "Year": (1900, 2100),
    "Month": (1, 12),
    "Day": (1, 31),
    "DayOfWeek": (1, 7),
    "Quarter": (1, 4),
    "WeekOfYear": (1, 53),
}


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


def assert_range(df: DataFrame, col_name: str, min_value: int, max_value: int) -> None:
    """Asserts range of integer column col_name"""
    outliers = df.filter((F.col(col_name) < min_value) | (F.col(col_name) > max_value))
    outliers_count = outliers.count()
    assert (
        outliers_count <= 0
    ), f"For column {col_name} there are {outliers_count} outliers from the range [{min_value}, {max_value}]"


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    schema_check = config["schemaCheck"]

    df = spark.read.load(input_dataset_path)
    df.cache()

    logging.info("Checking Date dimension")
    df.printSchema()

    # checking that none of the columns contain null
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

    # checking cardinalities
    for col, max_card in CHECK_SET_CARDINALITY_STRATEGY.items():
        cardinality = df.select(col).distinct().count()
        assert (
            cardinality <= max_card
        ), f"Column {col} cannot have more than {max_card} unique values"
        if cardinality < max_card:
            logging.warning(
                f"Column {col} has less than {max_card} unique values which is suspecious"
            )

    # checking ranges
    for col, (min_value, max_value) in CHECK_RANGE_STRATEGY.items():
        assert_range(df, col, min_value, max_value)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder.appName("Checking Date Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0] if len(sys.argv) > 0 else "s3a://dwp/staging/date.parquet"
    )
    schema_check = (
        sys.argv[1] in ["True", "true", "enabled", "yes"] if len(sys.argv) > 1 else True
    )

    config = {
        "inputDatasetPath": input_dataset_path,
        "schemaCheck": schema_check,
    }

    run(spark, config)

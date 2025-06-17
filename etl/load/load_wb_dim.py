import sys
import logging
from pyspark.sql import SparkSession
import pyspark.sql.types as T


NUMERIC_COL_POSTFIX = "Value"
LABEL_COL_POSTFIX = "Label"


COLUMNS_TO_DROP = [
    "Year",
    "WardCode"
]


COLUMNS = [
    # T.StructField("WardCode", T.StringType()), # DROPPED
    # T.StructField("Year", T.IntegerType()), # DROPPED
    T.StructField("LifeExpectancyValue", T.DoubleType(), True),
    T.StructField("ChildhoodObesityValue", T.DoubleType(), True),
    T.StructField("IncapacityBenefitValue", T.DoubleType(), True),
    T.StructField("UnemploymentValue", T.DoubleType(), True),
    T.StructField("CrimeValue", T.DoubleType(), True),
    T.StructField("DeliberateFiresValue", T.DoubleType(), True),
    T.StructField("GcsePointsValue", T.DoubleType(), True),
    T.StructField("UnauthorisedAbsenceValue", T.DoubleType(), True),
    T.StructField("DependentChildrenValue", T.DoubleType(), True),
    T.StructField("PublicTransportAccessValue", T.DoubleType(), True),
    T.StructField("HomesWithAccessValue", T.DoubleType(), True),
    T.StructField("LifeExpectancyLabel", T.StringType(), False),
    T.StructField("ChildhoodObesityLabel", T.StringType(), False),
    T.StructField("IncapacityBenefitLabel", T.StringType(), False),
    T.StructField("UnemploymentLabel", T.StringType(), False),
    T.StructField("CrimeLabel", T.StringType(), False),
    T.StructField("GcsePointsLabel", T.StringType(), False),
    T.StructField("PublicTransportAccessLabel", T.StringType(), False),
    T.StructField("DeliberateFiresLabel", T.StringType(), False),
    T.StructField("UnauthorisedAbsenceLabel", T.StringType(), False),
    T.StructField("DependentChildrenLabel", T.StringType(), False),
    T.StructField("HomesWithAccessLabel", T.StringType(), False),
    T.StructField("WellBeingID", T.StringType(), False)
]


def produce_schema(dimension_format: str) -> T.StructType:
    fields = COLUMNS
    match dimension_format:
        case "preserve-all":
            pass
        case "only-labels":
            fields = list(filter(lambda fld: fld.name.endswith(LABEL_COL_POSTFIX), fields))
        case "only-numeric":
            fields = list(filter(lambda fld: fld.name.endswith(NUMERIC_COL_POSTFIX), fields))
    return T.StructType(fields)


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_table_name = config["outputTableName"]
    dimension_format = config["dimensionFormat"]

    logging.info("Preparing Well-Being dimension for load...")

    well_being = spark.read.load(input_dataset_path)
    well_being = well_being.drop(*COLUMNS_TO_DROP)
    well_being_schema = produce_schema(dimension_format)
    well_being = spark.createDataFrame(well_being.rdd, schema=well_being_schema)

    logging.info("Writing Well-Being dimension to the data mart...")

    (
        well_being.write.mode("overwrite")
        .format("delta")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact", "true")
        .option("overwriteSchema", "true")
        .saveAsTable(output_table_name)
    )

    spark.sql(f"OPTIMIZE {output_table_name} ZORDER BY (WellBeingID)")
    spark.sql(f"DESCRIBE DETAIL {output_table_name}").show(truncate=False)

    logging.info("Loaded and optimized Well-Being")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder.appName("Well-Being Dimension Loader")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0]
        if len(sys.argv) > 0
else "s3a://dwp/staging/well-being-dimension.parquet"
    )
    output_table_name = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "well_being"
    )
    dimension_format = sys.argv[2] if len(sys.argv) > 2 else "preserve-all"

    config = {
        "inputDatasetPath": input_dataset_path,
        "outputTableName": output_table_name,
        "dimensionFormat": dimension_format
    }

    logging.info(f"Running Load Well-Being Dimension with config: {config}")

    run(spark, config)

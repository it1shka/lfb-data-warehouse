import sys
import logging
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pyspark.sql.types as T


DATASET_SCHEMA = T.StructType(
    [
        T.StructField("WardID", T.StringType(), nullable=False),
        T.StructField("BoroughCode", T.StringType(), nullable=False),
        T.StructField("BoroughName", T.StringType(), nullable=False),
        T.StructField("WardCode", T.StringType(), nullable=False),
        T.StructField("WardName", T.StringType(), nullable=False),
    ]
)


COLUMNS_TO_SELECT = [
    "IncGeo_BoroughCode",
    "ProperCase",
    "IncGeo_WardCode",
    "IncGeo_WardName",
]

RENAMING_STRATEGY = {
    "IncGeo_BoroughCode": "BoroughCode",
    "ProperCase": "BoroughName",
    "IncGeo_WardCode": "WardCode",
    "IncGeo_WardName": "WardName",
}


def add_hash_id(
    df: DataFrame, id_col_name: str, cols_to_track: list[str], bits: int = 256
) -> DataFrame:
    """Hashes `cols_to_track` and adds the hash as `id_col_name`"""
    combined_cols = F.concat_ws("|", *cols_to_track)
    return df.withColumn(id_col_name, F.sha2(combined_cols, bits))


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_dataset_path = config["outputDatasetPath"]

    df = spark.read.load(input_dataset_path)

    df = df.select(*COLUMNS_TO_SELECT)
    df = df.withColumnsRenamed(RENAMING_STRATEGY)

    df = df.filter(F.col("WardCode").isNotNull())

    df = df.groupBy("WardCode").agg(
        F.first("WardName", ignorenulls=True).alias("WardName"),
        F.first("BoroughName", ignorenulls=True).alias("BoroughName"),
        F.first("BoroughCode", ignorenulls=True).alias("BoroughCode"),
    )

    df = add_hash_id(
        df, "WardID", ["WardCode", "WardName", "BoroughName", "BoroughCode"]
    )

    # casting dataframe to a strict schema
    df = spark.createDataFrame(df.rdd, schema=DATASET_SCHEMA)

    df.write.mode("overwrite").parquet(output_dataset_path)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder.appName("Preparing Ward Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0]
        if len(sys.argv) > 0
        else "s3a://dwp/staging/lfb-calls-clean.parquet"
    )
    output_dataset_path = (
        sys.argv[1] if len(sys.argv) > 1 else "s3a://dwp/staging/ward-dimension.parquet"
    )

    config = {
        "inputDatasetPath": input_dataset_path,
        "outputDatasetPath": output_dataset_path,
    }

    run(spark, config)

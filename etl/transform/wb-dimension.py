import sys
import logging
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F


MODE_PRESERVE_ALL = "preserve-all"
MODE_ONLY_LABELS = "only-labels"
MODE_ONLY_NUMERIC = "only-numeric"


NUMERIC_COL_POSTFIX = "Value"
LABEL_COL_POSTFIX = "Label"


COLUMN_RENAMING_STRATEGY = {
    "Ward_Code": "WardCode",
    # numerical
    "Life_Expectancy": f"LifeExpectancy{NUMERIC_COL_POSTFIX}",
    "Childhood_Obesity": f"ChildhoodObesity{NUMERIC_COL_POSTFIX }",
    "Incapacity_Benefit": f"IncapacityBenefit{NUMERIC_COL_POSTFIX}",
    "Unemployment": f"Unemployment{NUMERIC_COL_POSTFIX}",
    "Crime": f"Crime{NUMERIC_COL_POSTFIX}",
    "GCSE_points": f"GcsePoints{NUMERIC_COL_POSTFIX}",
    "Public_Transport_Access": f"PublicTransportAccess{NUMERIC_COL_POSTFIX}",
    "Deliberate_Fires": f"DeliberateFires{NUMERIC_COL_POSTFIX}",
    "Unauthorised_Absence": f"UnauthorisedAbsence{NUMERIC_COL_POSTFIX}",
    "Dependent_children": f"DependentChildren{NUMERIC_COL_POSTFIX}",
    "Homes_with_access": f"HomesWithAccess{NUMERIC_COL_POSTFIX}",
    # labels / buckets
    "Life_Expectancy_Bucket": f"LifeExpectancy{LABEL_COL_POSTFIX}",
    "Childhood_Obesity_Bucket": f"ChildhoodObesity{LABEL_COL_POSTFIX}",
    "Incapacity_Benefit_Bucket": f"IncapacityBenefit{LABEL_COL_POSTFIX}",
    "Unemployment_Bucket": f"Unemployment{LABEL_COL_POSTFIX}",
    "Crime_Bucket": f"Crime{LABEL_COL_POSTFIX}",
    "GCSE_points_Bucket": f"GcsePoints{LABEL_COL_POSTFIX}",
    "Public_Transport_Access_Bucket": f"PublicTransportAccess{LABEL_COL_POSTFIX}",
    "Deliberate_Fires_Bucket": f"DeliberateFires{LABEL_COL_POSTFIX}",
    "Unauthorised_Absence_Bucket": f"UnauthorisedAbsence{LABEL_COL_POSTFIX}",
    "Dependent_children_Bucket": f"DependentChildren{LABEL_COL_POSTFIX}",
    "Homes_with_access_Bucket": f"HomesWithAccess{LABEL_COL_POSTFIX}",
}


NULL_REPLACEMENT = "Unknown"


def add_hash_id(
    df: DataFrame, id_col_name: str, cols_to_track: list[str], bits: int = 256
) -> DataFrame:
    """Hashes `cols_to_track` and adds the hash as `id_col_name`"""
    combined_cols = F.concat_ws("|", *cols_to_track)
    return df.withColumn(id_col_name, F.sha2(combined_cols, bits))


def run(spark: SparkSession, config: dict) -> None:
    input_dataset_path = config["inputDatasetPath"]
    output_dataset_path = config["outputDatasetPath"]
    dimension_format = config["dimensionFormat"]

    df = spark.read.load(input_dataset_path)

    # preparing dimension
    df = df.withColumnsRenamed(COLUMN_RENAMING_STRATEGY)
    label_columns = list(
        filter(lambda col: col.endswith(LABEL_COL_POSTFIX), df.columns)
    )
    numeric_columns = list(
        filter(lambda col: col.endswith(NUMERIC_COL_POSTFIX), df.columns)
    )
    df = df.fillna(NULL_REPLACEMENT, subset=label_columns)
    if dimension_format == MODE_ONLY_LABELS:
        df = df.drop(*numeric_columns)
    elif dimension_format == MODE_ONLY_NUMERIC:
        df = df.drop(*label_columns)
    df = add_hash_id(df, "WellBeingID", ["Year", "WardCode"])

    # incrementally writing
    try:
        existing_df = spark.read.load(output_dataset_path)
        logging.info(
            "Found existing well-being dimension, filtering out existing records"
        )

        new_df = df.join(existing_df, "WellBeingID", "left_anti")

        new_df_count = new_df.count()
        if new_df_count <= 0:
            logging.info("No new well-being information to add")
            return

        logging.info(f"Adding {new_df_count} new well-being records to dimension")
        new_df.write.mode("append").parquet(output_dataset_path)
        logging.info("New well-being records added successfully")

    except Exception:
        logging.warning(
            "Failed to incrementally add well-being records, rolling back to overwrite mode..."
        )
        df.write.mode("overwrite").parquet(output_dataset_path)
        logging.info("Well-being was created successfully")


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Well-Being Dimension Preparation")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    input_dataset_path = (
        sys.argv[0]
        if len(sys.argv) > 0
        else "s3a://dwp/staging/well-being-clean.parquet"
    )
    output_dataset_path = (
        sys.argv[1]
        if len(sys.argv) > 1
        else "s3a://dwp/staging/well-being-dimension.parquet"
    )
    dimension_format = sys.argv[2] if len(sys.argv) > 2 else MODE_PRESERVE_ALL

    config = {
        "inputDatasetPath": input_dataset_path,
        "outputDatasetPath": output_dataset_path,
        "dimensionFormat": dimension_format,
    }

    run(spark, config)

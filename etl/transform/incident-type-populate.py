import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when, lit, concat, coalesce, sha2

### The logic behind the incident type dimension:
# 1. Get three columns from the LFB calls dataset:
#    - IncidentGroup
#    - StopCodeDescription
#    - SpecialServiceType
# 2. Create an "IncidentType" equal to the incident group
#    - If IncidentGroup is "Special Service", set IncidentType to the contents of StopCodeDescription
# 3. Create an "IncidentDescription" equal to the StopCodeDescription
#    - If IncidentGroup is "Special Service", set IncidentDescription to the contents of SpecialServiceType
# 4. If the IncidentType is "Use of Special Operations Room", set IncidentDescription to "Use of Special Operations Room"
# 5. If the IncidentType is null, set it to "Unknown"
# 6. IF the IncidentDescription is null, set it to "Unknown incident type"
# 6. Add a surrogate key to the incident type dimension


# The logic needs to be the same as in the incident type dimension
def derive_incident_type_key(df: DataFrame) -> DataFrame:
    return (
        df.withColumn(
            "IncidentType",
            when(
                col("IncidentGroup") == "Special Service", col("StopCodeDescription")
            ).otherwise(col("IncidentGroup")),
        )
        .withColumn(
            "IncidentDescription",
            when(
                col("IncidentGroup") == "Special Service", col("SpecialServiceType")
            ).otherwise(col("StopCodeDescription")),
        )
        .withColumn(
            "IncidentDescription",
            when(
                col("IncidentType") == "Use of Special Operations Room",
                lit("Use of Special Operations Room"),
            ).otherwise(col("IncidentDescription")),
        )
        .withColumn(
            "IncidentType",
            coalesce(col("IncidentType"), lit("Unknown")),
        )
        .withColumn(
            "IncidentDescription",
            coalesce(col("IncidentDescription"), lit("Unknown incident type")),
        )
        .withColumn(
            "IncidentTypeKey",
            sha2(
                concat(
                    col("IncidentType"),
                    lit("|"),
                    col("IncidentDescription"),
                ),
                256,
            ),
        )
    )


def run(spark: SparkSession, config: dict) -> None:
    lfb_calls_path = config["lfbCallsCleanPath"]
    output_path = config["outputDatasetPath"]

    logging.info("Starting incident type dimension creation")

    try:
        logging.info(f"Reading LFB calls dataset from {lfb_calls_path}")
        lfb_df = spark.read.parquet(lfb_calls_path)
    except Exception as e:
        logging.error(f"Error reading LFB calls dataset: {e}")
        sys.exit(1)
    logging.info(f"Successfully read LFB calls dataset")

    relevant_df = (
        lfb_df.select("IncidentGroup", "StopCodeDescription", "SpecialServiceType")
        .distinct()
        .cache()
    )
    logging.info(
        f"Extracted {relevant_df.count()} distinct records for incident type dimension"
    )

    incident_type_df = (
        derive_incident_type_key(relevant_df)
        .select("IncidentType", "IncidentDescription", "IncidentTypeKey")
        .distinct()
    ).cache()
    logging.info(
        f"Created incident type dimension with {incident_type_df.count()} records"
    )

    # Add sentinel key for unknown/missing incident types
    sentinel_df = spark.createDataFrame(
        [("Unknown", "Unknown incident type", "Unknown")],
        ["IncidentType", "IncidentDescription", "IncidentTypeKey"],
    )

    # Union the sentinel record with the main dataset
    incident_type_with_sentinel_df = sentinel_df.union(incident_type_df)
    logging.info(f"Added sentinel key to incident type dimension")

    incident_type_with_sentinel_df.write.mode("overwrite").parquet(output_path)
    logging.info(f"Written incident type dimension to {output_path}")
    incident_type_with_sentinel_df.show(10)
    incident_type_with_sentinel_df.printSchema()


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Create Incident Type Dimension")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "lfbCallsCleanPath": (
            sys.argv[0]
            if len(sys.argv) > 0
            else "s3a://dwp/staging/lfb-calls-clean.parquet"
        ),
        "outputDatasetPath": (
            sys.argv[1]
            if len(sys.argv) > 1
            else "s3a://dwp/staging/incident-type.parquet"
        ),
    }

    run(spark, config)

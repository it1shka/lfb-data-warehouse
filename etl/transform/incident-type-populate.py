import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, concat, coalesce, abs, hash

### The logic behind the incident type dimension:
# 1. Get three columns from the LFB calls dataset:
#    - IncidentGroup
#    - StopCodeDescription
#    - SpecialServiceType
# 2. Create an "IncidentType" equal to the incident group
#    - If IncidentGroup is "Special Service", set IncidentType to the contents of StopCodeDescription
# 3. Create an "IncidentDescription" equal to the StopCodeDescription
#    - If IncidentGroup is "Special Service", set IncidentDescription to the contents of SpecialServiceType
# 4. Add a surrogate key to the incident type dimension


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
    logging.info(f"Successfully read LFB calls dataset with {lfb_df.count()} records")

    relevant_df = lfb_df.select(
        "IncidentGroup", "StopCodeDescription", "SpecialServiceType"
    ).distinct()
    logging.info(
        f"Extracted {relevant_df.count()} distinct records for incident type dimension"
    )

    incident_type_df = (
        relevant_df.withColumn(
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
        .select("IncidentType", "IncidentDescription")
        .distinct()
        .withColumn(
            "IncidentTypeKey",
            abs(
                hash(
                    concat(
                        coalesce(col("IncidentType"), lit("")),
                        lit("|"),
                        coalesce(col("IncidentDescription"), lit("")),
                    )
                )
            ),
        )
    )
    logging.info(
        f"Created incident type dimension with {incident_type_df.count()} records"
    )

    final_df = incident_type_df
    try:
        existing_df = spark.read.parquet(output_path)
        logging.info(f"Reading existing incident type dimension from {output_path}")
        final_df = existing_df.union(incident_type_df).dropDuplicates(
            ["IncidentTypeKey"]
        )
    except Exception as e:
        logging.error(f"Error reading existing incident type dimension: {e}")

    final_df.write.mode("overwrite").parquet(output_path)
    logging.info(f"Written incident type dimension to {output_path}")


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
            sys.argv[1] if len(sys.argv) > 1 else "s3a://dwp/staging/incident.parquet"
        ),
    }

    run(spark, config)

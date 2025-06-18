import logging
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as spark_sum,
    date_format,
    year,
    month,
    desc,
    asc,
    when,
    coalesce,
    lit,
    round as spark_round,
)

logging.basicConfig(level=logging.INFO)


def create_per_month(
    spark: SparkSession, fact_table_name: str, output_schema: str
) -> None:
    logging.info("Creating per month aggregate...")

    monthly_incidents = (
        spark.table(fact_table_name)
        .select("IncidentNumber", "NotionalCost", "NumCalls", "DateOfCall")
        .withColumn("Year", year("DateOfCall"))
        .withColumn("Month", month("DateOfCall"))
        .withColumn("YearMonth", date_format("DateOfCall", "yyyy-MM"))
        .groupBy("Year", "Month", "YearMonth")
        .agg(
            count("IncidentNumber").alias("IncidentCount"),
            spark_sum("NotionalCost").alias("TotalCost"),
            avg("NotionalCost").alias("AvgCost"),
            spark_sum("NumCalls").alias("TotalCalls"),
        )
        .withColumn("AvgCost", spark_round("AvgCost", 2))
        .orderBy("Year", "Month")
    )

    output_table = f"{output_schema}.per_month"
    monthly_incidents.write.mode("overwrite").format("delta").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logging.info(f"Created table: {output_table}")
    monthly_incidents.show(20, truncate=False)


def create_per_ward(
    spark: SparkSession, fact_table_name: str, output_schema: str
) -> None:
    logging.info("Creating per ward aggregate...")

    ward_incidents = (
        spark.table(fact_table_name)
        .select(
            "IncidentNumber",
            "NotionalCost",
            "NumCalls",
            "FirstPumpAttendanceTime",
            "WardID",
        )
        .groupBy("WardID")
        .agg(
            count("IncidentNumber").alias("IncidentCount"),
            spark_sum("NotionalCost").alias("TotalCost"),
            avg("NotionalCost").alias("AvgIncidentCost"),
            spark_sum("NumCalls").alias("TotalCalls"),
            avg("FirstPumpAttendanceTime").alias("AvgResponseTime"),
        )
        .withColumn("AvgResponseTime", spark_round("AvgResponseTime", 2))
        .orderBy(desc("IncidentCount"))
    )

    output_table = f"{output_schema}.per_ward"
    ward_incidents.write.mode("overwrite").format("delta").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logging.info(f"Created table: {output_table}")
    ward_incidents.show(20, truncate=False)


def create_types_per_ward(
    spark: SparkSession,
    fact_table_name: str,
    output_schema: str,
) -> None:
    logging.info("Creating incident types per ward aggregate...")

    incident_types_ward = (
        spark.table(fact_table_name)
        .select(
            "IncidentNumber",
            "NotionalCost",
            "WardID",
            "IncidentTypeKey",
        )
        .groupBy("WardID", "IncidentTypeKey")
        .agg(
            count("IncidentNumber").alias("IncidentCount"),
            spark_sum("NotionalCost").alias("TotalCost"),
            avg("NotionalCost").alias("AvgCost"),
        )
        .withColumn("AvgCost", spark_round("AvgCost", 2))
        .orderBy("WardID", desc("IncidentCount"))
    )

    output_table = f"{output_schema}.types_per_ward"
    incident_types_ward.write.mode("overwrite").format("delta").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logging.info(f"Created table: {output_table}")
    incident_types_ward.show(30, truncate=False)


def per_property_type(
    spark: SparkSession,
    fact_table_name: str,
    output_schema: str,
) -> None:
    logging.info("Creating average response time per location type aggregate...")

    response_times = (
        spark.table(fact_table_name)
        .select(
            "IncidentNumber",
            "NotionalCost",
            "PumpsAttending",
            "FirstPumpAttendanceTime",
            "LocationTypeKey",
        )
        .filter(
            col("FirstPumpAttendanceTime").isNotNull()
            & (col("FirstPumpAttendanceTime") > 0)
        )
        .groupBy("LocationTypeKey")
        .agg(
            count("IncidentNumber").alias("IncidentCount"),
            avg("FirstPumpAttendanceTime").alias("AvgResponseTimeMinutes"),
            spark_sum("NotionalCost").alias("TotalCost"),
            avg("NotionalCost").alias("AvgCost"),
            spark_sum("PumpsAttending").alias("TotalPumpsAttending"),
            avg("PumpsAttending").alias("AvgPumpsAttending"),
        )
        .withColumn("AvgResponseTimeMinutes", spark_round("AvgResponseTimeMinutes", 2))
        .withColumn("AvgCost", spark_round("AvgCost", 2))
        .withColumn("AvgPumpsAttending", spark_round("AvgPumpsAttending", 2))
        .orderBy(desc("AvgResponseTimeMinutes"))
    )

    output_table = f"{output_schema}.per_location_type"
    response_times.write.mode("overwrite").format("delta").option(
        "overwriteSchema", "true"
    ).saveAsTable(output_table)

    logging.info(f"Created table: {output_table}")
    response_times.show(30, truncate=False)


def run(spark: SparkSession, config: dict) -> None:
    """Main function to create all aggregate tables"""
    fact_table_name = config["factTableName"]
    output_schema = config["outputSchema"]

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {output_schema}")
    logging.info(f"Ensured schema '{output_schema}' exists")

    create_per_month(spark, fact_table_name, output_schema)
    create_per_ward(spark, fact_table_name, output_schema)
    create_types_per_ward(spark, fact_table_name, output_schema)
    per_property_type(spark, fact_table_name, output_schema)

    logging.info(
        f"All aggregate tables created successfully in schema '{output_schema}'"
    )
    spark.sql(f"SHOW TABLES IN {output_schema}").show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Load Sample Aggregates")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "factTableName": sys.argv[0] if len(sys.argv) > 0 else "lfb_call",
        "outputSchema": sys.argv[1] if len(sys.argv) > 1 else "analytics",
    }

    logging.info(f"Running Load Sample Aggregates with config: {config}")

    run(spark, config)

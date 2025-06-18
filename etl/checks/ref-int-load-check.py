import sys
import logging
from pyspark.sql import SparkSession


def run(spark: SparkSession, config: dict) -> None:
    fact_table: str = config["factTable"]
    dimension_links: dict[str, str] = config["dimensionLinks"]

    fact_df = spark.table(fact_table).cache()

    logging.info(f"Checking {fact_table} for referential integrity with dimensions...")

    violated = False
    for dim_table, foreign_key in dimension_links.items():
        dim_df = spark.table(dim_table)
        violation_df = (
            fact_df.select(foreign_key)
            .distinct()
            .join(dim_df, on=foreign_key, how="left_anti")
        ).cache()

        violation_count = violation_df.count()
        if violation_count > 0:
            logging.error(f"Found violation for {dim_table} (fk: {foreign_key}):")
            violation_df.show(truncate=False)
            logging.error(
                f"{violation_count} fact records do not have a corresponding row in the dimension {dim_table}"
            )
            violated = True

    if violated:
        raise Exception("Failed due to referential integrity violation")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    spark = (
        SparkSession.builder.appName("Integrity Check")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    assert (
        len(sys.argv) >= 3
    ), "This job expects exactly 3 arguments: fact table, dimension tables (delimited with comma), foreigh keys (delimited with comma)"
    fact_table = sys.argv[0]
    dimension_tables = sys.argv[1].split(",")
    foreign_keys = sys.argv[2].split(",")

    dimension_links = {
        dim_tbl: fk for dim_tbl, fk in zip(dimension_tables, foreign_keys)
    }

    config = {
        "factTable": fact_table,
        "dimensionLinks": dimension_links,
    }

    run(spark, config)

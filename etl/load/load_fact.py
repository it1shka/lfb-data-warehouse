import logging
import sys
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql.functions import (
    col,
    when,
    lit,
    concat,
    coalesce,
    sha2,
    to_timestamp,
    date_format,
    unix_seconds,
    floor,
    row_number,
    abs,
    year,
)
from pyspark.sql.types import (
    StringType,
    StructType,
    StructField,
    IntegerType,
    DoubleType,
    DateType,
)

logging.basicConfig(level=logging.INFO)

BUCKET_SIZE = 43200  # 12 hours in seconds

COLUMNS_TO_KEEP_STAGE_1 = [
    "IncidentNumber",
    "DateOfCall",
    "TimeOfCall",
    "IncidentGroup",
    "StopCodeDescription",
    "StopCodeDescription",
    "SpecialServiceType",
    "SpecialServiceType",
    "PropertyType",
    "IncGeo_WardCode",
    "IncGeo_WardName",
    "Latitude",
    "Longitude",
    "IncidentStationGround",
    "FirstPumpArriving_AttendanceTime",
    "FirstPumpArriving_DeployedFromStation",
    "SecondPumpArriving_AttendanceTime",
    "SecondPumpArriving_DeployedFromStation",
    "NumStationsWithPumpsAttending",
    "NumPumpsAttending",
    "PumpMinutesRounded",
    "Notional Cost (£)",
    "NumCalls",
]

COLUMNS_TO_DROP_POST_STAGE_1 = [
    "IncidentGroup",
    "StopCodeDescription",
    "SpecialServiceType",
    "IncidentType",
    "IncidentDescription",
]

COLUMNS_TO_DROP_BEFORE_FINAL = [
    "WardCode",
    "YearOfCall",
    "IncGeo_WardName",
    "PropertyCategory",
    "SecondPumpArriving_AttendanceTime",
    "SecondPumpArriving_DeployedFromStation",
]

FINAL_SCHEMA = [
    StructField("DateOfCall", DateType(), nullable=False),
    StructField("IncidentNumber", StringType(), nullable=False),
    StructField("TimeOfCall", StringType(), nullable=False),
    StructField("Latitude", DoubleType(), nullable=True),
    StructField("Longitude", DoubleType(), nullable=True),
    StructField("IncidentStationGround", StringType(), nullable=False),
    StructField("FirstPumpAttendanceTime", IntegerType(), nullable=True),
    StructField("FirstPumpStation", StringType(), nullable=True),
    StructField("StationsWithPumpsAttending", IntegerType(), nullable=False),
    StructField("PumpsAttending", IntegerType(), nullable=False),
    StructField("PumpMinutes", IntegerType(), nullable=False),
    StructField("NotionalCost", IntegerType(), nullable=False),
    StructField("NumCalls", IntegerType(), nullable=False),
    StructField("IncidentTypeKey", StringType(), nullable=False),
    StructField("LocationTypeKey", StringType(), nullable=False),
    StructField("WardID", StringType(), nullable=False),
    StructField("AirQualityKey", StringType(), nullable=False),
    StructField("WeatherKey", StringType(), nullable=False),
    StructField("WellBeingID", StringType(), nullable=False),
]


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
    input_dataset_path = config["inputDatasetPath"]
    location_type_table_name = config["locationTypeTableName"]
    ward_dimension_table_name = config["wardDimensionTableName"]
    air_quality_dataset_path = config["airQualityDatasetPath"]
    weather_dataset_path = config["weatherDatasetPath"]
    well_being_dataset_path = config["wellBeingDatasetPath"]
    output_table_name = config["outputTableName"]

    logging.info("Reading lfb dataset...")
    lfb_df = (
        spark.read.parquet(input_dataset_path).select(*COLUMNS_TO_KEEP_STAGE_1).cache()
    )
    logging.info(f"Read {lfb_df.count()} rows from {input_dataset_path}")

    # STAGE 1: Derive incident type keys
    logging.info("Deriving incident type keys for fact table...")
    lfb_with_incident_key_df = (
        derive_incident_type_key(lfb_df)
        .withColumn(
            "IncidentTypeKey",
            when(
                (col("IncidentType") == lit("Unknown"))
                & (col("IncidentDescription") == lit("Unknown incident type")),
                lit("Unknown"),
            ).otherwise(col("IncidentTypeKey")),
        )
        .drop(*COLUMNS_TO_DROP_POST_STAGE_1)
    )

    # STAGE 2: Join with the location type dimension
    logging.info(f"Reading location type dimension table {location_type_table_name}...")
    location_type_df = spark.table(location_type_table_name).cache()
    logging.info(
        f"Read {location_type_df.count()} rows from location type dimension table {location_type_table_name}"
    )
    logging.info("Joining with location type dimension...")
    lfb_with_location_df = (
        lfb_with_incident_key_df.join(
            location_type_df,
            "PropertyType",
            "left",
        )
        .withColumn("LocationTypeKey", coalesce(col("LocationTypeKey"), lit("Unknown")))
        .drop("PropertyType")
    )
    location_type_df.unpersist()

    # STAGE 3: Join with the ward dimension
    logging.info("Reading ward dimension table...")
    ward_dimension_df = spark.table(ward_dimension_table_name).cache()
    logging.info(
        f"Read {ward_dimension_df.count()} rows from ward dimension table {ward_dimension_table_name}"
    )
    logging.info("Joining with ward dimension...")
    lfb_with_ward_df = (
        lfb_with_location_df.withColumnRenamed("IncGeo_WardCode", "WardCode")
        .join(
            ward_dimension_df,
            "WardCode",
            "left",
        )
        .withColumn("WardID", coalesce(col("WardID"), lit("Unknown")))
        .drop("WardName", "BoroughName", "BoroughCode")
    )
    ward_dimension_df.unpersist()

    # STAGE 4: Join with air quality dimension
    # This will join two datasets with a different measurement precision
    # This will be done by creating a time bucket of ~ +-12 hours
    # If no measurement is found in the ~ +-12 hours, it will be set to Unknown
    logging.info("Reading air quality dimension table...")
    aq_dimension_df = (
        spark.read.parquet(air_quality_dataset_path)
        .select(["ReadingDateTime", "AirQualityKey"])
        .cache()
    )
    logging.info(
        f"Read {aq_dimension_df.count()} rows from air quality dimension table {air_quality_dataset_path}"
    )
    logging.info("Joining with air quality dimension...")

    tmp_lfb = lfb_with_ward_df.select(["IncidentNumber", "DateOfCall", "TimeOfCall"])

    lfb_aq_prep = tmp_lfb.withColumn(
        "CallUnixEpoch",
        unix_seconds(
            to_timestamp(
                concat(
                    col("DateOfCall"),
                    lit(" "),
                    date_format(col("TimeOfCall"), "HH:mm:ss"),
                ),
                "yyyy-MM-dd HH:mm:ss",
            )
        ),
    )

    # Bucket creation (adding neighboring buckets, to avoid missing edge cases)
    lfb_aq_prep = (
        lfb_aq_prep.withColumn("TimeBucket", floor(col("CallUnixEpoch") / BUCKET_SIZE))
        .union(
            lfb_aq_prep.withColumn(
                "TimeBucket",
                floor(col("CallUnixEpoch") / BUCKET_SIZE) + 1,
            )
        )
        .union(
            lfb_aq_prep.withColumn(
                "TimeBucket",
                floor(col("CallUnixEpoch") / BUCKET_SIZE) - 1,
            )
        )
    )

    # Convert air quality reading date time to unix epoch and create time buckets
    aq_dimension_df_prep = aq_dimension_df.withColumn(
        "ReadingUnixEpoch", unix_seconds(col("ReadingDateTime"))
    ).withColumn("TimeBucket", floor(col("ReadingUnixEpoch")) / BUCKET_SIZE)

    # Join the two datasets on the time bucket and calculate the time difference
    lfb_aq_joined = (
        lfb_aq_prep.join(
            aq_dimension_df_prep,
            "TimeBucket",
            "left",
        )
        .withColumn(
            "TimeDifference",
            abs(col("CallUnixEpoch") - col("ReadingUnixEpoch")),
        )
        .filter(col("TimeDifference") <= BUCKET_SIZE)
    )

    # Select the closest air quality reading for each incident
    lfb_aq_pairs = (
        lfb_aq_joined.withColumn(
            "row_num",
            row_number().over(
                Window.partitionBy("IncidentNumber").orderBy("TimeDifference")
            ),
        )
        .filter(col("row_num") == 1)
        .select(
            "IncidentNumber",
            "AirQualityKey",
        )
    )

    # Join the air quality pairs with the main lfb dataframe
    lfb_with_aq_df = lfb_with_ward_df.join(
        lfb_aq_pairs,
        "IncidentNumber",
        "left",
    ).withColumn("AirQualityKey", coalesce(col("AirQualityKey"), lit("Unknown")))
    aq_dimension_df.unpersist()

    # STAGE 5: Load the weather dimension
    logging.info("Reading weather dimension table...")
    weather_dimension_df = (
        spark.read.parquet(weather_dataset_path)
        .select(["date", "WeatherKey"])
        .withColumnRenamed("date", "DateOfCall")
        .cache()
    )
    logging.info(
        f"Read {weather_dimension_df.count()} rows from weather dimension table {weather_dataset_path}"
    )
    lfb_with_weather = lfb_with_aq_df.join(
        weather_dimension_df,
        "DateOfCall",
        "left",
    ).withColumn("WeatherKey", coalesce(col("WeatherKey"), lit("Unknown")))
    weather_dimension_df.unpersist()

    # STAGE 6: Load the the well-being dimension
    logging.info("Reading well-being dimension table...")
    well_being_dimension_df = (
        spark.read.parquet(well_being_dataset_path)
        .select(["WardCode", "Year", "WellBeingID"])
        .withColumnRenamed("Year", "YearOfCall")
        .cache()
    )
    logging.info(
        f"Read {well_being_dimension_df.count()} rows from well-being dimension table {well_being_dataset_path}"
    )
    lfb_with_well_being = (
        lfb_with_weather.withColumn("YearOfCall", year("DateOfCall"))
        .join(
            well_being_dimension_df,
            ["WardCode", "YearOfCall"],
            "left",
        )
        .withColumn("WellBeingID", coalesce(col("WellBeingID"), lit("Unknown")))
        .cache()
    )
    well_being_dimension_df.unpersist()

    # STAGE 7: Finalize the fact table
    logging.info("Finalizing the fact table...")
    final_df = (
        lfb_with_well_being.drop(*COLUMNS_TO_DROP_BEFORE_FINAL)
        .withColumnsRenamed(
            {
                "FirstPumpArriving_AttendanceTime": "FirstPumpAttendanceTime",
                "FirstPumpArriving_DeployedFromStation": "FirstPumpStation",
                "Notional Cost (£)": "NotionalCost",
                "NumStationsWithPumpsAttending": "StationsWithPumpsAttending",
                "NumPumpsAttending": "PumpsAttending",
                "PumpMinutesRounded": "PumpMinutes",
            }
        )
        .withColumn("TimeOfCall", date_format(col("TimeOfCall"), "HH:mm:ss"))
        .withColumn(
            "IncidentStationGround",
            coalesce(col("IncidentStationGround"), lit("Unknown")),
        )
        .withColumn(
            "StationsWithPumpsAttending",
            coalesce(col("StationsWithPumpsAttending"), lit(0)),
        )
        .withColumn(
            "PumpsAttending",
            coalesce(col("PumpsAttending"), lit(0)),
        )
        .withColumn(
            "PumpMinutes",
            coalesce(col("PumpMinutes"), lit(0)),
        )
        .withColumn(
            "NotionalCost",
            coalesce(col("NotionalCost"), lit(0)),
        )
        .withColumn(
            "NumCalls",
            coalesce(col("NumCalls"), lit(1)),
        )
    )
    final_df = spark.createDataFrame(final_df.rdd, schema=StructType(FINAL_SCHEMA))

    final_df.show()
    final_df.printSchema()

    logging.info("Writing to Delta table with optimizations...")
    final_df.write.mode("overwrite").format("delta").option(
        "delta.autoOptimize.optimizeWrite", "true"
    ).option("delta.autoOptimize.autoCompact", "true").option(
        "overwriteSchema", "true"
    ).partitionBy(
        "IncidentStationGround"
    ).saveAsTable(
        output_table_name
    )

    spark.sql(
        f"OPTIMIZE {output_table_name} ZORDER BY (DateOfCall, NotionalCost)"
    )
    logging.info(f"Fact table '{output_table_name}' created and optimized successfully")

    spark.sql(f"DESCRIBE DETAIL {output_table_name}").show(truncate=False)


if __name__ == "__main__":
    spark = (
        SparkSession.builder.appName("Load The Fact Table")
        .enableHiveSupport()
        .getOrCreate()
    )

    logging.info(f"Running with args: {sys.argv}")

    config = {
        "inputDatasetPath": (
            sys.argv[0]
            if len(sys.argv) > 0
            else "s3a://dwp/staging/lfb-calls-clean.parquet"
        ),
        "locationTypeTableName": "location_type" if len(sys.argv) > 1 else sys.argv[1],
        "wardDimensionTableName": ("ward" if len(sys.argv) > 2 else sys.argv[2]),
        "airQualityDatasetPath": (
            sys.argv[3]
            if len(sys.argv) > 3
            else "s3a://dwp/staging/air-quality-clean.parquet"
        ),
        "weatherDatasetPath": (
            sys.argv[4]
            if len(sys.argv) > 4
            else "s3a://dwp/staging/weather-clean.parquet"
        ),
        "wellBeingDatasetPath": (
            sys.argv[5]
            if len(sys.argv) > 5
            else "s3a://dwp/staging/well-being-dimension.parquet"
        ),
        "outputTableName": "lfb_call",
    }

    logging.info(f"Running Load The Fact Table with config: {config}")

    run(spark, config)

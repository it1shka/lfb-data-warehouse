from pyspark.sql import SparkSession, DataFrame

COLUMNS_TO_DROP = [
  "CalYear",
  "HourOfCall",
  "AddressQualifier",
  "Postcode_district",
  "UPRN",
  "IncGeo_BoroughName",
  "IncGeo_WardNameNew",
  "Easting_m",
  "Northing_m",
  "Easting_rounded",
  "Northing_rounded",
  "FRS",
  "PumpCount"
]

def run(spark: SparkSession, config: dict) -> None:
  df = spark.read.option("header", "true").csv(config["lfb_dataset_path"])

  # Dropping unnecessary columns and removing "NULL"s
  df = df.drop(*COLUMNS_TO_DROP)
  df = df.replace(to_replace="NULL", value=None)

  # Inferring the schema after the change
  df.write.mode("overwrite").option("header", "true").csv(config["temp_csv_path"])
  df = spark.read\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .csv(config["temp_csv_path"])

  df.printSchema()

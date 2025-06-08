from pyspark.sql import SparkSession


def run(spark: SparkSession, config: dict) -> None:
    # Create a DataFrame with sample data
    data = [("Alice", 1), ("Bob", 2), ("Cathy", 3)]
    df = spark.createDataFrame(data, ["name", "value"])

    # Show the DataFrame
    df.show()

    # Perform a simple transformation
    transformed_df = df.withColumn("value_squared", df["value"] ** 2)

    # Show the transformed DataFrame
    transformed_df.show()
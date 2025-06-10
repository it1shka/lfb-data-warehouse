from airflow.sdk import dag


@dag(schedule=None, tags=["example"])
def example_spark_livy_pipeline() -> None:
    """
    Treat this one as an example.
    It shows how to properly call Spark jobs from Airflow.
    """
    ...

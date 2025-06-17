from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.operators.empty import EmptyOperator
import logging

BATCH = 1

# Resilience Configuration
DEFAULT_RETRIES = 5
RETRY_DELAY = timedelta(minutes=2)
RETRY_EXPONENTIAL_BACKOFF = True
TASK_TIMEOUT = timedelta(hours=2)
POLLING_INTERVAL = 10  # seconds


# Enhanced Spark Configuration for resilience
SPARK_CONF = {
    "spark.shuffle.compress": "false",
    "fs.s3a.endpoint": "ilum-minio:9000",
    "fs.s3a.access.key": "minioadmin",
    "fs.s3a.secret.key": "minioadmin",
    "fs.s3a.connection.ssl.enabled": "false",
    "fs.s3a.path.style.access": "true",
    "fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "fs.s3a.impl.disable.cache": "true",
    "fs.s3a.bucket.ilum-mlflow.fast.upload": "true",
    "spark.com.amazonaws.sdk.disableCertChecking": "true",
    "spark.sql.adaptive.enabled": "true",
    "spark.sql.adaptive.coalescePartitions.enabled": "true",
    "spark.hadoop.fs.s3a.connection.timeout": "200000",
    "spark.hadoop.fs.s3a.attempts.maximum": "10",
    "spark.kubernetes.executor.deleteOnTermination": "false",
    "spark.kubernetes.executor.pods.gracefulDeletionTimeout": "120s",
    "spark.task.maxFailures": "3",
    "spark.stage.maxConsecutiveAttempts": "8",
    "spark.sql.execution.arrow.maxRecordsPerBatch": "10000",
    "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
    "spark.sql.adaptive.skewJoin.enabled": "true",
    "spark.kubernetes.allocation.batch.size": "5",
    "spark.kubernetes.allocation.batch.delay": "1s",
    "spark.executor.memoryFraction": "0.8",
    "spark.network.timeout": "800s",
    "spark.executor.heartbeatInterval": "60s",
}


def task_failure_callback(context):
    task_instance = context["task_instance"]

    logging.error(f"Task {task_instance.task_id} in DAG {task_instance.dag_id} failed")
    logging.error(f"Try number: {task_instance.try_number}")
    logging.error(f"Max tries: {task_instance.max_tries}")

    if task_instance.try_number >= task_instance.max_tries:
        logging.error(
            f"Task {task_instance.task_id} failed after {task_instance.max_tries} attempts"
        )


def custom_livy_operator(
    task_id: str,
    file_path: str,
    args: list,
    retries: int = DEFAULT_RETRIES,
    retry_delay: timedelta = RETRY_DELAY,
    timeout: timedelta = TASK_TIMEOUT,
    polling_interval: int = POLLING_INTERVAL,
) -> LivyOperator:
    """Create a LivyOperator with enhanced resilience settings"""
    return LivyOperator(
        task_id=task_id,
        file=file_path,
        polling_interval=polling_interval,
        livy_conn_id="ilum-livy-proxy",
        conf=SPARK_CONF,
        args=args,
        retries=retries,
        retry_delay=retry_delay,
        retry_exponential_backoff=RETRY_EXPONENTIAL_BACKOFF,
        execution_timeout=timeout,
        on_failure_callback=task_failure_callback,
        pool="default_pool",
        max_active_tis_per_dag=2,
    )


with DAG(
    dag_id="main_fire_brigade_pipeline",
    schedule_interval=None,
    start_date=datetime(2025, 6, 15),
    catchup=False,
    tags=["dwp", "fire-brigade"],
    max_active_runs=1,
    max_active_tasks=2,
    concurrency=2,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": DEFAULT_RETRIES,
        "retry_delay": RETRY_DELAY,
        "retry_exponential_backoff": RETRY_EXPONENTIAL_BACKOFF,
        "execution_timeout": TASK_TIMEOUT,
        "on_failure_callback": task_failure_callback,
    },
    dagrun_timeout=timedelta(hours=2),
) as dag:
    with TaskGroup(group_id="extract_stage") as extract_stage:
        with TaskGroup(group_id="extract_step") as extract_step:
            lfb_extract = custom_livy_operator(
                task_id="lfb_extract",
                file_path="s3a://dwp/jobs/extract/lfb-extract.py",
                args=[
                    f"s3a://dwp/batches/{BATCH}/lfb-calls.csv",
                    "s3a://dwp/staging/lfb-calls.parquet",
                ],
            )
            aq_extract = custom_livy_operator(
                task_id="aq_extract",
                file_path="s3a://dwp/jobs/extract/aq-extract.py",
                args=[
                    f"s3a://dwp/batches/{BATCH}/air-quality",
                    "s3a://dwp/staging/air-quality.parquet",
                ],
            )
            wb_extract = custom_livy_operator(
                task_id="wb_extract",
                file_path="s3a://dwp/jobs/extract/wb-extract.py",
                args=[
                    f"s3a://dwp/batches/{BATCH}/well-being.csv",
                    "s3a://dwp/staging/well-being.parquet",
                ],
            )
            weather_extract = custom_livy_operator(
                task_id="weather_extract",
                file_path="s3a://dwp/jobs/extract/weather-extract.py",
                args=[
                    f"s3a://dwp/batches/{BATCH}/weather.csv",
                    "s3a://dwp/staging/weather.parquet",
                ],
            )
        with TaskGroup(group_id="extract_check_step") as extract_check_step:
            lfb_extract_check = custom_livy_operator(
                task_id="lfb_extract_check",
                file_path="s3a://dwp/jobs/checks/post-extract-check.py",
                args=[
                    "s3a://dwp/staging/lfb-calls.parquet",
                    "39",
                    "IncidentNumber",
                ],
            )
            aq_extract_check = custom_livy_operator(
                task_id="aq_extract_check",
                file_path="s3a://dwp/jobs/checks/post-extract-check.py",
                args=[f"s3a://dwp/staging/air-quality.parquet", "6", ""],
            )
            wb_extract_check = custom_livy_operator(
                task_id="wb_extract_check",
                file_path="s3a://dwp/jobs/checks/post-extract-check.py",
                args=[f"s3a://dwp/staging/well-being.parquet", "16", ""],
            )
            weather_extract_check = custom_livy_operator(
                task_id="weather_extract_check",
                file_path="s3a://dwp/jobs/checks/post-extract-check.py",
                args=[f"s3a://dwp/staging/weather.parquet", "11", "date"],
            )
    with TaskGroup(group_id="transform_stage") as transform_stage:
        with TaskGroup(group_id="cleanse_step") as cleanse_step:
            lfb_cleanse = custom_livy_operator(
                task_id="lfb_cleanse",
                file_path="s3a://dwp/jobs/transform/lfb-cleanse.py",
                args=[
                    f"s3a://dwp/staging/lfb-calls.parquet",
                    "s3a://dwp/staging/lfb-calls-clean.parquet",
                ],
            )
            aq_cleanse = custom_livy_operator(
                task_id="aq_cleanse",
                file_path="s3a://dwp/jobs/transform/aq-cleanse.py",
                args=[
                    f"s3a://dwp/staging/air-quality.parquet",
                    "s3a://dwp/staging/air-quality-clean.parquet",
                ],
            )
            wb_cleanse = custom_livy_operator(
                task_id="wb_cleanse",
                file_path="s3a://dwp/jobs/transform/wb-cleanse.py",
                args=[
                    "s3a://dwp/staging/well-being.parquet",
                    "s3a://dwp/staging/well-being-clean.parquet",
                ],
            )
            weather_cleanse = custom_livy_operator(
                task_id="weather_cleanse",
                file_path="s3a://dwp/jobs/transform/weather-cleanse.py",
                args=[
                    "s3a://dwp/staging/weather.parquet",
                    "s3a://dwp/staging/weather-clean.parquet",
                ],
            )
        with TaskGroup(group_id="prepare_dimensions") as prepare_dimensions_step:
            prepare_date_dimension = custom_livy_operator(
                task_id="prepare_date_dimension",
                file_path="s3a://dwp/jobs/transform/date-dimension.py",
                args=[
                    "s3a://dwp/staging/lfb-calls-clean.parquet",
                    "s3a://dwp/staging/date-dimension.parquet",
                ],
            )
            prepare_ward_dimension = custom_livy_operator(
                task_id="prepare_ward_dimension",
                file_path="s3a://dwp/jobs/transform/ward-dimension.py",
                args=[
                    "s3a://dwp/staging/lfb-calls-clean.parquet",
                    "s3a://dwp/staging/ward-dimension.parquet",
                ],
            )
            incident_type_populate = custom_livy_operator(
                task_id="incident_type_populate",
                file_path="s3a://dwp/jobs/transform/incident-type-populate.py",
                args=[
                    "s3a://dwp/staging/lfb-calls-clean.parquet",
                    "s3a://dwp/staging/incident-type-dimension.parquet",
                ],
            )
            location_type_populate = custom_livy_operator(
                task_id="location_type_populate",
                file_path="s3a://dwp/jobs/transform/derive-location-types.py",
                args=[
                    "s3a://dwp/staging/lfb-calls-clean.parquet",
                    "s3a://dwp/staging/location-types.parquet",
                ],
            )
            prepare_well_being_dimension = custom_livy_operator(
                task_id="prepare_well_being_dimension",
                file_path="s3a://dwp/jobs/transform/wb-dimension.py",
                args=[
                    "s3a://dwp/staging/well-being-clean.parquet",
                    "s3a://dwp/staging/well-being-dimension.parquet",
                    "preserve-all",
                ],
            )
        with TaskGroup(group_id="check_dimensions") as check_dimensions_step:
            check_ward_dimension = custom_livy_operator(
                task_id="check_ward_dimension",
                file_path="s3a://dwp/jobs/checks/ward-dimension-check.py",
                args=["s3a://dwp/staging/ward-dimension.parquet", "True"],
            )
            check_date_dimension = custom_livy_operator(
                task_id="check_date_dimension",
                file_path="s3a://dwp/jobs/checks/date-dimension-check.py",
                args=["s3a://dwp/staging/date-dimension.parquet", "True"],
            )
            check_incident_type_dimension = custom_livy_operator(
                task_id="check_incident_type_dimension",
                file_path="s3a://dwp/jobs/checks/incident-type-dimension-check.py",
                args=["s3a://dwp/staging/incident-type-dimension.parquet", "True"],
            )
            check_well_being_dimension = custom_livy_operator(
                task_id="check_well_being_dimension",
                file_path="s3a://dwp/jobs/checks/wb-dimension-check.py",
                args=["s3a://dwp/staging/well-being-dimension.parquet"],
            )
            check_location_type_dimension = custom_livy_operator(
                task_id="check_location_type_dimension",
                file_path="s3a://dwp/jobs/checks/location-type-dimension-check.py",
                args=["s3a://dwp/staging/location-types.parquet", "True"],
            )
    with TaskGroup(group_id="load_stage") as load_stage:
        with TaskGroup(group_id="load_dimensions_step") as load_dimensions_step:
            load_date_dimension = custom_livy_operator(
                task_id="load_date_dimension",
                file_path="s3a://dwp/jobs/load/load_date_dim.py",
                args=[
                    "s3a://dwp/staging/date-dimension.parquet",
                    "default.date",
                ],
            )
            load_incident_types = custom_livy_operator(
                task_id="load_incident_types",
                file_path="s3a://dwp/jobs/load/load_incident_types.py",
                args=[
                    "s3a://dwp/staging/incident-type-dimension.parquet",
                    "default.incident_types",
                ],
            )
            load_weather = custom_livy_operator(
                task_id="load_weather",
                file_path="s3a://dwp/jobs/load/load_weather_dim.py",
                args=[
                    "s3a://dwp/staging/weather-clean.parquet",
                    "default.weather",
                ],
            )
            load_air_quality = custom_livy_operator(
                task_id="load_air_quality",
                file_path="s3a://dwp/jobs/load/load_air_quality_dim.py",
                args=[
                    "s3a://dwp/staging/air-quality-clean.parquet",
                    "default.air_quality",
                ],
            )
            load_location_types = custom_livy_operator(
                task_id="location_type_load",
                file_path="s3a://dwp/jobs/load/load_location_type.py",
                args=[
                    "s3a://dwp/staging/location-types.parquet",
                    "default.location_type",
                ],
            )
            load_ward = custom_livy_operator(
                task_id="load_ward",
                file_path="s3a://dwp/jobs/load/load_ward_dim.py",
                args=[
                    "s3a://dwp/staging/ward-dimension.parquet",
                    "default.ward",
                ],
            )
            load_well_being = custom_livy_operator(
                task_id="load_well_being",
                file_path="s3a://dwp/jobs/load/load_wb_dim.py",
                args=[
                    "s3a://dwp/staging/well-being-dimension.parquet",
                    "default.well_being",
                ],
            )

        load_fact = custom_livy_operator(
            task_id="load_fact_table",
            file_path="s3a://dwp/jobs/load/load_fact.py",
            args=[
                "s3a://dwp/staging/lfb-calls-clean.parquet",
                "location_type",
                "ward",
                "s3a://dwp/staging/air-quality-clean.parquet",
                "s3a://dwp/staging/weather-clean.parquet",
                "s3a://dwp/staging/well-being-dimension.parquet",
            ],
        )
    # setting up dependencies
    pipeline_start = EmptyOperator(task_id="pipeline_start")
    extract_end_transform_start = EmptyOperator(task_id="extract_end_transform_start")
    transform_end_load_start = EmptyOperator(task_id="transform_end_load_start")
    load_end = EmptyOperator(task_id="load_end")

    # Extract
    pipeline_start >> [lfb_extract, aq_extract, wb_extract, weather_extract]
    lfb_extract >> lfb_extract_check
    aq_extract >> aq_extract_check
    wb_extract >> wb_extract_check
    weather_extract >> weather_extract_check
    extract_end_transform_start << [
        lfb_extract_check,
        aq_extract_check,
        wb_extract_check,
        weather_extract_check,
    ]

    # Transform
    extract_end_transform_start >> [
        weather_cleanse,
        aq_cleanse,
        lfb_cleanse,
        wb_cleanse,
    ]
    lfb_cleanse >> [
        prepare_date_dimension,
        prepare_ward_dimension,
        incident_type_populate,
        location_type_populate,
    ]
    wb_cleanse >> prepare_well_being_dimension

    prepare_ward_dimension >> check_ward_dimension
    prepare_date_dimension >> check_date_dimension
    incident_type_populate >> check_incident_type_dimension
    prepare_well_being_dimension >> check_well_being_dimension
    location_type_populate >> check_location_type_dimension

    transform_end_load_start << [
        weather_cleanse,
        aq_cleanse,
        check_date_dimension,
        check_incident_type_dimension,
        check_ward_dimension,
        check_well_being_dimension,
        check_location_type_dimension,
    ]

    # Load
    transform_end_load_start >> [
        load_date_dimension,
        load_incident_types,
        load_weather,
        load_air_quality,
        load_location_types,
        load_ward,
        load_well_being,
    ]

    load_fact << [
        load_date_dimension,
        load_incident_types,
        load_weather,
        load_air_quality,
        load_location_types,
        load_ward,
        load_well_being,
    ]

    load_fact >> load_end

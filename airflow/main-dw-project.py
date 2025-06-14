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


class MetaTask:
    """Groups tasks into one unit of work by defining an entry point and an exit point"""

    def __init__(self, task_group_name: str, dag: DAG) -> None:
        self.entry = EmptyOperator(task_id=f"{task_group_name}__entry", dag=dag)
        self.finish = EmptyOperator(task_id=f"{task_group_name}__finish", dag=dag)

    def short_circuit(self) -> MetaTask:
        self.entry.set_downstream(self.finish)
        return self

    def set_upstream(self, other: MetaTask) -> None:
        self.entry.set_upstream(other.finish)

    def set_downstream(self, other: MetaTask) -> None:
        self.finish.set_downstream(other.entry)

    def wrap(self, task_or_list_of_tasks) -> MetaTask:
        self.entry.set_downstream(task_or_list_of_tasks)
        self.finish.set_upstream(task_or_list_of_tasks)
        return self

    def enter_step(self, other: MetaTask) -> None:
        self.entry.set_downstream(other.entry)

    def finish_step(self, other: MetaTask) -> None:
        self.finish.set_upstream(other.finish)

    def wrap_steps(self, step_list: list[MetaTask]) -> MetaTask:
        assert len(step_list) > 0
        self.enter_step(step_list[0])
        for i in range(len(step_list) - 1):
            step_a, step_b = step_list[i : i + 2]
            step_b.set_upstream(step_a)
        self.finish_step(step_list[-1])
        return self


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

        extract_stage = MetaTask("extract_stage", dag).wrap(
            [lfb_extract, aq_extract, wb_extract, weather_extract]
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
            cleanse_step = MetaTask("cleanse_step", dag).wrap(
                [lfb_cleanse, aq_cleanse, wb_cleanse, weather_cleanse]
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
            # TODO: here add other dimensions
            prepare_dimensions_step = MetaTask("prepare_dimensions", dag).wrap(
                [prepare_date_dimension, prepare_ward_dimension, incident_type_populate]
            )

        transform_stage = MetaTask("transform_stage", dag).wrap_steps(
            [cleanse_step, prepare_dimensions_step]
        )

    transform_stage.set_upstream(extract_stage)

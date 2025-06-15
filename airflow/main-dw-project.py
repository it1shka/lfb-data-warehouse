from __future__ import annotations
from datetime import datetime
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.apache.livy.operators.livy import LivyOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

BATCH = 1
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
}


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
) as dag:

    with TaskGroup(group_id="extract_stage") as extract_stage:
        lfb_extract = LivyOperator(
            task_id="lfb_extract",
            file="s3a://dwp/jobs/extract/lfb-extract.py",
            polling_interval=5,
            livy_conn_id="ilum-livy-proxy",
            conf=SPARK_CONF,
            args=[
                f"s3a://dwp/batches/{BATCH}/lfb-calls.csv",
                "s3a://dwp/staging/lfb-calls.parquet",
            ],
        )
        aq_extract = LivyOperator(
            task_id="aq_extract",
            file="s3a://dwp/jobs/extract/aq-extract.py",
            polling_interval=5,
            livy_conn_id="ilum-livy-proxy",
            conf=SPARK_CONF,
            args=[
                f"s3a://dwp/batches/{BATCH}/air-quality",
                "s3a://dwp/staging/air-quality.parquet",
            ],
        )
        wb_extract = LivyOperator(
            task_id="wb_extract",
            file="s3a://dwp/jobs/extract/wb-extract.py",
            polling_interval=5,
            livy_conn_id="ilum-livy-proxy",
            conf=SPARK_CONF,
            args=[
                f"s3a://dwp/batches/{BATCH}/well-being.csv",
                "s3a://dwp/staging/well-being.parquet",
            ],
        )
        weather_extract = LivyOperator(
            task_id="weather_extract",
            file="s3a://dwp/jobs/extract/weather-extract.py",
            polling_interval=5,
            livy_conn_id="ilum-livy-proxy",
            conf=SPARK_CONF,
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
            lfb_cleanse = LivyOperator(
                task_id="lfb_cleanse",
                file="s3a://dwp/jobs/transform/lfb-cleanse.py",
                polling_interval=5,
                livy_conn_id="ilum-livy-proxy",
                conf=SPARK_CONF,
                args=[
                    f"s3a://dwp/staging/lfb-calls.parquet",
                    "s3a://dwp/staging/lfb-calls-clean.parquet",
                ],
            )
            aq_cleanse = LivyOperator(
                task_id="aq_cleanse",
                file="s3a://dwp/jobs/transform/aq-cleanse.py",
                polling_interval=5,
                livy_conn_id="ilum-livy-proxy",
                conf=SPARK_CONF,
                args=[
                    f"s3a://dwp/staging/air-quality.parquet",
                    "s3a://dwp/staging/air-quality-clean.parquet",
                ],
            )
            wb_cleanse = LivyOperator(
                task_id="wb_cleanse",
                file="s3a://dwp/jobs/transform/wb-cleanse.py",
                polling_interval=5,
                livy_conn_id="ilum-livy-proxy",
                conf=SPARK_CONF,
                args=[
                    "s3a://dwp/staging/well-being.parquet",
                    "s3a://dwp/staging/well-being-clean.parquet",
                ],
            )
            weather_cleanse = LivyOperator(
                task_id="weather_cleanse",
                file="s3a://dwp/jobs/transform/weather-cleanse.py",
                polling_interval=5,
                livy_conn_id="ilum-livy-proxy",
                conf=SPARK_CONF,
                args=[
                    "s3a://dwp/staging/weather.parquet",
                    "s3a://dwp/staging/weather-clean.parquet",
                ],
            )
            cleanse_step = MetaTask("cleanse_step", dag).wrap(
                [lfb_cleanse, aq_cleanse, wb_cleanse, weather_cleanse]
            )

        with TaskGroup(group_id="prepare_dimensions") as prepare_dimensions_step:
            prepare_date_dimension = LivyOperator(
                task_id="prepare_date_dimension",
                file="s3a://dwp/jobs/transform/date-dimension.py",
                polling_interval=5,
                livy_conn_id="ilum-livy-proxy",
                conf=SPARK_CONF,
                args=[
                    "s3a://dwp/staging/lfb-calls-clean.parquet",
                    "s3a://dwp/staging/date-dimension.parquet",
                ],
            )
            prepare_ward_dimension = LivyOperator(
                task_id="prepare_ward_dimension",
                file="s3a://dwp/jobs/transform/ward-dimension.py",
                polling_interval=5,
                livy_conn_id="ilum-livy-proxy",
                conf=SPARK_CONF,
                args=[
                    "s3a://dwp/staging/lfb-calls-clean.parquet",
                    "s3a://dwp/staging/ward-dimension.parquet",
                ],
            )
            # TODO: here add other dimensions
            prepare_dimensions_step = MetaTask("prepare_dimensions", dag).wrap(
                [prepare_date_dimension, prepare_ward_dimension]
            )

        transform_stage = MetaTask("transform_stage", dag).wrap_steps(
            [cleanse_step, prepare_dimensions_step]
        )

    transform_stage.set_upstream(extract_stage)

from datetime import datetime
from airflow import DAG
from airflow.providers.apache.livy.operators.livy import LivyOperator

STAGING_PATH = "s3a://dwp/staging"
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

with DAG(
    dag_id="main_dw_project",
    schedule_interval=None,
    start_date=datetime(2025, 6, 1),
    catchup=False,
    params={"batch": 1},
) as dag:

    lfb_extraction = LivyOperator(
        task_id="lfb_extraction_task",
        file="s3a://dwp/jobs/lfb-extract.py",
        polling_interval=5,
        livy_conn_id="ilum-livy-proxy",
        conf=SPARK_CONF,
        args=[
            f"s3a://dwp/batches/{dag.params['batch']}/lfb-calls.csv",
            STAGING_PATH,
        ],
    )

    aq_extraction = LivyOperator(
        task_id="aq_extraction_task",
        file="s3a://dwp/jobs/aq-extract.py",
        polling_interval=5,
        livy_conn_id="ilum-livy-proxy",
        conf=SPARK_CONF,
        args=[
            f"s3a://dwp/batches/{dag.params['batch']}/air-quality",
            STAGING_PATH,
        ],
    )

    wb_extraction = LivyOperator(
        task_id="wb_extraction_task",
        file="s3a://dwp/jobs/wb-extract.py",
        polling_interval=5,
        livy_conn_id="ilum-livy-proxy",
        conf=SPARK_CONF,
        args=[
            f"s3a://dwp/batches/{dag.params['batch']}/well-being.csv",
            STAGING_PATH,
        ],
    )

    weather_extraction = LivyOperator(
        task_id="weather_extraction_task",
        file="s3a://dwp/jobs/weather-extract.py",
        polling_interval=5,
        livy_conn_id="ilum-livy-proxy",
        conf=SPARK_CONF,
        args=[
            f"s3a://dwp/batches/{dag.params['batch']}/weather.csv",
            STAGING_PATH,
        ],
    )

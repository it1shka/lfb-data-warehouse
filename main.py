import sys
import os
import yaml
import importlib.util
from pyspark.sql import SparkSession


def fetch_spark_module(spark_job_name: str) -> tuple[object, True] | tuple[str, False]:
  """
  Dynamically imports a Spark job module from /jobs.
  """
  job_file_path = os.path.join("jobs", f"{spark_job_name}.py")
  if not os.path.exists(job_file_path):
    return f"Job file {job_file_path} does not exist.", False
  spec = importlib.util.spec_from_file_location(spark_job_name, job_file_path)
  job_module = importlib.util.module_from_spec(spec)
  spec.loader.exec_module(job_module)
  if not hasattr(job_module, 'run'):
    return f"Job {spark_job_name} does not have a 'run' function.", False
  return job_module, True


def fetch_spark_config(spark_job_name: str) -> tuple[dict, True] | tuple[str, False]:
  """
  Dynamically imports a YAML config file from /configs.
  """
  config_file_path = os.path.join("configs", f"{spark_job_name}.yaml")
  if not os.path.exists(config_file_path):
    return f"Config file {config_file_path} does not exist.", False
  with open(config_file_path, 'r') as file:
    try:
      config = yaml.safe_load(file)
      if 'job' not in config or 'spark' not in config:
        return "Config file must contain 'job' and 'spark' keys.", False
      return config, True
    except yaml.YAMLError as e:
      return f"Error parsing YAML config: {e}", False


def run_spark_module(spark_job_name: str, spark_module: object, config: dict) -> None:
  """
  1. Applies every key, value pair from config["spark"] for the spark session
  2. Calls the spark module "run" function passing spark session and config["job"]
  """
  builder = SparkSession.builder.appName(spark_job_name)
  for key, value in config["spark"].items():
    builder = builder.config(key, value)
  spark_session = builder.getOrCreate()
  spark_module.run(spark_session, config["job"])


def main() -> None:
  """
  1. Selects Spark job from /jobs
  2. Selects YAML config from /configs
  3. Runs the specified job with the given config
  """
  if len(sys.argv) != 2:
    print("Usage: python main.py <spark_job_name>")
    sys.exit(1)
  spark_job_name = sys.argv[1]
  spark_job_module, success = fetch_spark_module(spark_job_name)
  if not success:
    print(spark_job_module)
    sys.exit(1)
  config, success = fetch_spark_config(spark_job_name)
  if not success:
    print(config)
    sys.exit(1)
  run_spark_module(spark_job_name, spark_job_module, config)


if __name__ == "__main__":
  main()
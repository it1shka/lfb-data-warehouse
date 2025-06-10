## Project Setup

This project uses `pyenv` and `pyenv-virtualenv` to manage
python virtual environments. To run the project locally,
you will need an environment called `sparkenv`.

Here is the way how to create it:
```bash
# install pyenv and pyenv-virtualenv first, then run:
pyenv virtualenv 3.12 sparkenv
pyenv shell sparkenv
pip install pyspark
```

Then, every time you will run Python commands in this repository, Python will automatically detect the
`.python-version` file and run code in the proper environment

The guide I was using personally:
[PySpark Installation Guide](https://sparkbyexamples.com/pyspark/how-to-install-pyspark-on-mac/)

## How things work

### Apache Spark

Locally everything works in the following way:

1. You write Spark jobs using Python and put them into `jobs/` folder
2. You write YAML configs for your Spark jobs and put them into `configs/` folder
3. You put files you want to use in your Spark jobs into the `data/` folder
4. Your spark jobs treat `job-output/` folder as a staging area

To run a Spark job, use:
```bash
python main.py <spark_job_name>

# Example
python main.py lfb-preprocessing
```

### Apache Airflow

#### General Installation

Airflow is an orchestration tool that allows you to build
pipelines with Python. We will use Spark for ETL process itself
(mostly data manipulations) and Airflow for composing our Spark jobs
together such that they can accomplish more complex problems.

To run Airflow locally, you will first need to install it for our python virtual environment:
```bash
pyenv shell sparkenv
pip install apache-airflow[postgres,celery]
```

Then, there is a script in the root folder of this repo
called `launch-airflow.sh`. It forces Airflow to think that
its root is located in the local `airflow/` subdirectory,
when in reality env variable $AIRFLOW_HOME is normally set as
`~/airflow`.

This script temporarily changes the env variable such that
we can successfully launch Airflow from our local repo.

If you are using Linux/MacOS, you need to make script
executable and run it:
```bash
chmod +x ./launch-airflow.sh
./launch-airflow.sh
```

If you are using Windows, you need to do the same thing
but using Windows CMD or Powershell or write a similar script
using Powershell / Batch.

#### Apache Livy

Apache Spark and Apache Airflow are two distinct technologies
that are not working together out-of-the-box. We need some sort of a
middle man that would allow us to call Spark jobs from Airflow.

To accomplish actual work, Airflow uses operators. Each airflow task
can be represented as an operator.

We are building a version of our data warehouse that will work
both in local environment and on a cluster. While it's totally possible
to use Spark Submit operator, Python and/or Bash operators for Airflow
in the context of local testing, on a cluster Jan will be using Apache Livy operator.

Therefore we will be using `Apache Livy` since nobody wants to maintain
two different versions.

Apache Livy is essentially a REST API that allows us to control Spark
via REST requests.

Apache Livy requires a true Spark installation (for now, we were using PySpark -
in fact it's just a mock, not a real Spark engine) and additionally you have
to install Livy itself.

Therefore, the easiest way would be running these tools together on Docker.
Fortunately there is a ready Docker compose setup that we can utilize:
[Spark+Livy on Docker](https://github.com/Renien/docker-spark-livy)

Obviously you will need to install Docker and a plugin for Docker
called Docker Compose. Also you need to make sure that the docker daemon
is running in the background.

All you need to do is to clone the directory and hit docker-compose up:
```bash
git clone https://github.com/Renien/docker-spark-livy
cd docker-spark-livy
docker-compose up
```

If you are on Linux a sudo before docker-compose may be required.

The last thing you'll need to do is to install Apache Livy operator for Airflow:
```bash
pyenv shell sparkenv
pip install apache-airflow-providers-apache-livy
```

Airflow is running locally, Apache Spark and Apache Livy are running on Docker.

#### Resource Management

For outputs, we need to consider the following cases:

1. Running jobs without Airflow: PySpark writes simply to the local files - already implemented
2. Running jobs with local Airflow: Spark writes to an attached Docker volume
3. Running jobs with Airflow on cluster: Spark writes to S3 buckets


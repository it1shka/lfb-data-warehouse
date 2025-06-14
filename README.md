## Project Setup

Most of the project work will be done on the cluster,
therefore the setup is mostly about installing dependencies
for better Python autocompletion.

This project uses `pyenv` and `pyenv-virtualenv` to manage
python virtual environments. We will create an environment
called `dwp`, install all our dependencies there and activate it.

This is an example how to do that:
```bash
# install pyenv and pyenv-virtualenv first, then run:
pyenv virtualenv 3.12 dwp
pyenv shell dwp
pip install pandas PyYAML pyspark apache-airflow apache-airflow-providers-apache-livy
```

If you see that some module is highlighted red, just install it like:
```bash
pyenv shell dwp
pip install <module-name>
```

## Repository Structure

1. `airflow/` contains Airflow DAG(s) for our pipeline(s)
2. `etl/` contains Spark jobs for ETL process
3. `batching/` contains scripts to split original dataset into batches.
This is needed to simulate typical data warehouse use-case: historical
load + a couple of subsequent loads

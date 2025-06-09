#!/bin/bash

AIRFLOW_DIR="$(pwd)/airflow"
export AIRFLOW_HOME="$AIRFLOW_DIR"

sudo airflow standalone

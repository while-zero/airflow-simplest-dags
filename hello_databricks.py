from datetime import datetime
from typing import Any

from airflow import DAG
from airflow.providers.databricks.operators.databricks import \
    DatabricksRunNowOperator

# author - Piotrek Janeczek

# in order to run a databricks notebook from airflow
# you must configure databricks connection in Airflow:
# Admin -> Connections -> Add new record -> Databricks
# (if Databricks does not appear on the dropdown list, you 
# must install it first: e.g.
# pip install apache-airflow-providers-databricks )
# set following fields: 
# Connection Id = databricks_default 
#    (otherwise, the connection must be specified in each DAG)
# Host = https://asdasdasdasdasdasda.azuredatabricks.net/
# login = token
#    yup, just type "token"
# password = your private access token from databricks



conf_json: dict[str, Any] = {
    "job_id": 123456789123456789,    #this line is crucial
    "notebook_params": {}
}

with DAG(dag_id="hello_world_db_dag",
         start_date=datetime(2021,1,1),
         schedule_interval="@hourly",
         catchup=False) as dag:

    notebook_run = DatabricksRunNowOperator(
        task_id = 'run_notebook',
        json = conf_json
    )

    notebook_run

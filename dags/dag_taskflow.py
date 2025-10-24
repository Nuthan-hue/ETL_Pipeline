# dag_taskflow.py
import pendulum
from airflow import DAG
from airflow.sdk import task

with DAG(
    dag_id="taskflow_smoke",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
):
    @task
    def hello():
        return "ok"

    hello()

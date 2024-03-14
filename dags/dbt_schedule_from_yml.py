import os
import re
import yaml
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

CURRENT_DIR = os.getcwd()
DBT_DIR = CURRENT_DIR + '/dags/dbt/partna_warehouse'


def make_dbt_pipeline(schedule):
    dag_id = "dbt_dag_basic_yml_" + schedule.get("name", "")
    dag = DAG(
        dag_id,
        start_date=datetime(2020, 12, 23),
        description=schedule.get("description", ""),
        schedule_interval=schedule.get("interval"),
        catchup=False,
    )

    task_id = re.sub("[^a-zA-Z0-9_]+", "_", schedule.get("name", ""))
    task_run = BashOperator(
        dag=dag,
        task_id=f"{task_id}_run",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --select { schedule.get('selector').strip() }"
    )

    task_test = BashOperator(
        dag=dag,
        task_id=f"{task_id}_test",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --select { schedule.get('selector').strip() }"
    )

    task_run >> task_test
    globals()[dag_id] = dag


CUR_DIR = os.path.abspath(os.path.dirname(__file__))
filepath = f"{CUR_DIR}/schedulers.yaml"
if os.path.exists(filepath):
    with open(filepath, "r") as f:
        schedule_conf = yaml.safe_load(f)
        schedulers = schedule_conf["schedulers"]
        for schedule in schedulers:
            make_dbt_pipeline(schedule)

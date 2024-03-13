import os
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

CURRENT_DIR = os.getcwd()
DBT_DIR = CURRENT_DIR + '/dags/dbt/partna_warehouse'

dag = DAG(
    "dbt_dag_basic",
    start_date=datetime(2020, 12, 23),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule_interval='@daily',
    catchup=False,
)

task_run = BashOperator(
    dag=dag,
    task_id="dbt_run",
    bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir ."
)

task_test = BashOperator(
    dag=dag,
    task_id="dbt_test",
    bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir ."
)

task_run >> task_test

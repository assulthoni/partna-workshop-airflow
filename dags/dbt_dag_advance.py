import os
import json
from datetime import datetime
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

CURRENT_DIR = os.getcwd()
DBT_DIR = CURRENT_DIR + '/dags/dbt/partna_warehouse'

dag = DAG(
    "dbt_dag_advance",
    start_date=datetime(2020, 12, 23),
    description="A sample Airflow DAG to invoke dbt runs using a BashOperator",
    schedule_interval='@daily',
    catchup=False,
)


def load_manifest():
    local_filepath = f"{DBT_DIR}/target/manifest.json"
    with open(local_filepath) as f:
        data = json.load(f)
        return data


def make_dbt_task(node, dbt_verb):
    model = node.split(".")[-1]
    if dbt_verb == "run":
        dbt_task = BashOperator(
            dag=dag,
            task_id=node,
            bash_command=(
                f"cd {DBT_DIR} && "
                f"dbt {dbt_verb} --models {model} "
                f"--profiles-dir ."
            ),
        )
    elif dbt_verb == "test":
        node_test = node.replace("model", "test")
        dbt_task = BashOperator(
            dag=dag,
            task_id=node_test,
            bash_command=(
                f"cd {DBT_DIR} && "
                f"dbt {dbt_verb} --models {model} "
                f"--profiles-dir ."
            ),
        )
    return dbt_task


dbt_compile = BashOperator(
    dag=dag,
    task_id='dbt_compile',
    bash_command=(
        f"cd {DBT_DIR} && "
        f"dbt compile "
        f"--profiles-dir ."
    ),
)

data = load_manifest()
dbt_tasks = {}

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")
        dbt_tasks[node] = make_dbt_task(node, "run")
        dbt_tasks[node_test] = make_dbt_task(node, "test")

for node in data["nodes"].keys():
    if node.split(".")[0] == "model":
        node_test = node.replace("model", "test")
        dbt_compile >> dbt_tasks[node] >> dbt_tasks[node_test]
        for upstream_node in data["nodes"][node]["depends_on"]["nodes"]:
            upstream_node_type = upstream_node.split(".")[0]
            if upstream_node_type == "model":
                dbt_compile >> dbt_tasks[upstream_node] >> dbt_tasks[node]

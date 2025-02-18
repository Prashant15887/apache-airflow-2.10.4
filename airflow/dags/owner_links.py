from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="example_dag_owners",
    start_date=datetime(2022, 8, 5),
    schedule=None,
    owner_links={"airflow": "https://airflow.apache.org"},
):
    BashOperator(task_id="task_using_linked_owner", bash_command="echo 1", owner="airflow")
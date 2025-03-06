from airflow.utils.timezone import datetime
from custom_operator.hello_db_operator import HelloDBOperator
from airflow.models.dag import DAG

with DAG(
    "custom_operator_2",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    hello_db_task = HelloDBOperator(task_id="sample-task", name="foo_bar", postgres_conn_id="postgres_airflow_db")
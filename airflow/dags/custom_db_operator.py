from airflow.utils.timezone import datetime
from custom_operators.hello_db_operator import HelloDBOperator
from airflow.models.dag import DAG

with DAG(
    "custom_db_operator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    hello_db_task = HelloDBOperator(task_id="sample-custom-db-operator-task", name="foo_bar", postgres_conn_id="postgres_airflow_db")
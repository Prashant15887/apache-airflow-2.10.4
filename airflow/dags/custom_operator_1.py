from airflow.utils.timezone import datetime
from custom_operator.hello_operator import HelloOperator
from airflow.models.dag import DAG

with DAG(
    "custom_operator_1",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    hello_task = HelloOperator(task_id="sample-task", name="foo_bar")
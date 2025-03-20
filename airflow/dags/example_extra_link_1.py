from airflow.models.dag import DAG
from airflow.utils.timezone import datetime
from custom_operators.operator_extra_link_1 import MyFirstOperator

with DAG(
    dag_id="example_extra_link_1",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    link_test = MyFirstOperator(
        task_id="link_test",
    )
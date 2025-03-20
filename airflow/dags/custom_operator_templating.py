from airflow.utils.timezone import datetime
from custom_operators.hello_operator_templating import HelloOperator
from airflow.models.dag import DAG

with DAG(
    "custom_operator_templating",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    hello_task = HelloOperator(
        task_id="custom_operator_templating_task",
        name="{{ task_instance.task_id }}",
        world="Earth",
    )
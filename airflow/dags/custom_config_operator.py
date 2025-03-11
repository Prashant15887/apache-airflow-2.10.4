from airflow.models.dag import DAG
from airflow.utils.timezone import datetime
from custom_operator.configuration_operator import MyConfigOperator

with DAG(
    dag_id="custom_config_operator",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:

    config_task = MyConfigOperator(
        task_id="custom_config_operator_task",
        configuration={"query": {"job_id": "123", "sql": "select * from my_table"}},
    )
from airflow.utils.timezone import datetime
# from custom_operator.hello_operator_test import HelloOperator
from airflow.models.dag import DAG
from custom_operator.hello_operator_test import MyHelloOperator

with DAG(
    "custom_operator_test",
    schedule=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["example"],
) as dag:
    hello_task = MyHelloOperator(
        task_id="custom_operator_test_task",
        field_a="Testing limitations"
    )
    # hello_task = HelloOperator(
    #     task_id="custom_operator_test_task",
    #     # field_a_id="Testing limitations"
    #     field_a = "Testing limitations"
    # )
    # hello_task = HelloOperator(
    #     task_id="custom_operator_test_task",
    #     field_a="Testing limitations a",
    #     field_b="Testing limitations b"
    # )
    # hello_task = MyHelloOperator(
    #     task_id="custom_operator_test_task",
    #     field_a="Testing limitations a",
    #     field_b="Testing limitations b"
    # )
    # hello_task = HelloOperator(
    #     task_id="custom_operator_test_task",
    #     field_a="TESTING LIMITATIONS"
    # )
    # hello_task = MyHelloOperator(
    #     task_id="custom_operator_test_task",
    #     name="{{ task_instance.task_id }}",
    #     world="{{ var.value.my_world }}",
    # )
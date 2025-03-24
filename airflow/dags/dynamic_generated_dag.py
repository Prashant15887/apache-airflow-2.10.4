from datetime import datetime
from airflow.decorators import dag, task
from airflow.utils.dag_parsing_context import get_parsing_context

configs = {
    "config1": {"message": "first DAG will receive this message"},
    "config2": {"message": "second DAG will receive this message"},
}

current_dag_id = get_parsing_context().dag_id

for config_name, config in configs.items():
    dag_id = f"dynamic_generated_dag_{config_name}"

    if current_dag_id is not None and current_dag_id != dag_id:
        continue  # skip generation of non-selected DAG

    @dag(
        dag_id=dag_id,
        start_date=datetime(2022, 2, 1),
        schedule=None,
        catchup=False
    )
    def dynamic_generated_dag():
        @task
        def print_message(message):
            print(message)

        print_message(config["message"])

    dynamic_generated_dag()
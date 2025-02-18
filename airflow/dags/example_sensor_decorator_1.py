import pendulum

from airflow.decorators import dag, task
from airflow.sensors.base import PokeReturnValue
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def example_sensor_decorator_1():
    # Using a sensor operator to wait for the upstream data to be ready.
    @task.sensor(poke_interval=60, timeout=3600, mode="reschedule")
    def wait_for_upstream() -> PokeReturnValue:
        return PokeReturnValue(is_done=True, xcom_value="xcom_value")
    @task
    def dummy_operator() -> None:
        pass
    wait_for_upstream() >> dummy_operator()
tutorial_etl_dag = example_sensor_decorator_1()

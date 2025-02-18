from airflow.decorators import dag, task
from airflow.sensors.filesystem import FileSensor
import pendulum
import pandas as pd

@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def example_task_dependency():
    @task()
    def extract_from_file():
        """
        #### Extract from file task
        A simple Extract task to get data ready for the rest of the data
        pipeline, by reading the data from a file into a pandas dataframe
        """
        order_data_file = "/Users/prashantsingh/workspace/apache-airflow-2.10.4/airflow/dags/files/order_data.csv"
        order_data_df = pd.read_csv(order_data_file)
        # print(order_data_df)
        return order_data_df


    file_task = FileSensor(task_id="check_file", filepath="/Users/prashantsingh/workspace/apache-airflow-2.10.4/airflow/dags/files/order_data.csv")
    order_data = extract_from_file()

    file_task >> order_data

extract_file_dag = example_task_dependency()

import pendulum
import requests

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

API = "https://opendata.fmi.fi/timeseries"

aq_fields = {
    "fmisid": "int32",
    "time": "datetime64[ns]",
    "AQINDEX_PT1H_avg": "float64",
    "PM10_PT1H_avg": "float64",
    "PM25_PT1H_avg": "float64",
    "O3_PT1H_avg": "float64",
    "CO_PT1H_avg": "float64",
    "SO2_PT1H_avg": "float64",
    "NO2_PT1H_avg": "float64",
    "TRSC_PT1H_avg": "float64",
}
base = ObjectStoragePath("gs://google_cloud_default@airflow-tutorial-data-1")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
)
def tutorial_objectstorage_1():
    """
    ### Object Storage Tutorial Documentation
    This is a tutorial DAG to showcase the usage of the Object Storage API.
    Documentation that goes along with the Airflow Object Storage tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/objectstorage.html)
    """
    @task
    def get_air_quality_data(**kwargs) -> ObjectStoragePath:
        """
        #### Get Air Quality Data
        This task gets air quality data from the Finnish Meteorological Institute's
        open data API. The data is saved as parquet.
        """
        import pandas as pd

        execution_date = kwargs["logical_date"]
        start_time = kwargs["data_interval_start"]

        params = {
            "format": "json",
            "precision": "double",
            "groupareas": "0",
            "producer": "airquality_urban",
            "area": "Uusimaa",
            "param": ",".join(aq_fields.keys()),
            "starttime": start_time.isoformat(timespec="seconds"),
            "endtime": execution_date.isoformat(timespec="seconds"),
            "tz": "UTC",
        }

        response = requests.get(API, params=params)
        response.raise_for_status()

        # ensure the bucket exists
        base.mkdir(exist_ok=True)

        formatted_date = execution_date.format("YYYYMMDD")
        path = base / f"air_quality_{formatted_date}.parquet"

        df = pd.DataFrame(response.json()).astype(aq_fields)
        with path.open("wb") as file:
            df.to_parquet(file)

        return path
    @task
    def analyze(path: ObjectStoragePath, **kwargs):
        """
        #### Analyze
        This task analyzes the air quality data, prints the results
        """
        import duckdb

        conn = duckdb.connect(database=":memory:")
        conn.register_filesystem(path.fs)
        conn.execute(f"CREATE OR REPLACE TABLE airquality_urban AS SELECT * FROM read_parquet('{path}')")

        df2 = conn.execute("SELECT * FROM airquality_urban").fetchdf()

        print(df2.head())
    obj_path = get_air_quality_data()
    analyze(obj_path)
tutorial_objectstorage_1()
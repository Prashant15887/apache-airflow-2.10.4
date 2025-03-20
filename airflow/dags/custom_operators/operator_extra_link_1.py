from airflow.models.baseoperator import BaseOperator
from google_link import GoogleLink

class MyFirstOperator(BaseOperator):
    operator_extra_links = (GoogleLink(),)

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def execute(self, context):
        self.log.info("Hello World!")
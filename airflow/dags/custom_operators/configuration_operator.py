from collections.abc import Sequence
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


class MyConfigOperator(BaseOperator):
    template_fields: Sequence[str] = ("configuration",)
    template_fields_renderers = {
        "configuration": "json",
        "configuration.query.sql": "sql",
    }

    def __init__(self, configuration: dict, **kwargs) -> None:
        super().__init__(**kwargs)
        self.configuration = configuration

    def execute(self, context: Context) -> Any:
        print(self.configuration)
        print(self.configuration["query"]["sql"])
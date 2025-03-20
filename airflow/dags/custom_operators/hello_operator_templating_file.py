from collections.abc import Sequence

from airflow.models.baseoperator import BaseOperator

class HelloOperator(BaseOperator):
    template_fields: Sequence[str] = ("guest_name",)
    template_ext = ".sql"

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.guest_name = name

    def execute(self, context):
        print(self.guest_name)
        return self.guest_name
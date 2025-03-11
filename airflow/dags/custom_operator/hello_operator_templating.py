from collections.abc import Sequence
from airflow.models.baseoperator import BaseOperator

class HelloOperator(BaseOperator):
    template_fields: Sequence[str] = ("name",)

    def __init__(self, name: str, world: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.world = world

    def execute(self, context):
        message = f"Hello {self.world} it's {self.name}!"
        print(message)
        return message
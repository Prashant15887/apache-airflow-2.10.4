from airflow.models.baseoperator import BaseOperator


class HelloOperator(BaseOperator):
    ui_color = "#ff0000"
    ui_fgcolor = "#000000"
    custom_operator_name = "Howdy"

    def __init__(self, name: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name

    def execute(self, context):
        message = f"Hello {self.name}"
        print(message)
        return message
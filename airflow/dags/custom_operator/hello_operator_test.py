# from collections.abc import Sequence
from typing import Any

from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context


# class HelloOperator(BaseOperator):
#     template_fields = "field_a"
#
#     # def __init__(self, field_a_id: str, **kwargs) -> None:  # <- should be def __init__(field_a)-> None
#     #     super().__init__(**kwargs)
#     #     self.field_a = field_a_id  # <- should be self.field_a = field_a
#
#     def __init__(self, field_a: str, **kwargs) -> None:  # <- should be def __init__(field_a)-> None
#         super().__init__(**kwargs)
#         self.field_a = field_a  # <- should be self.field_a = field_a
#
#     def execute(self, context):
#         print(self.field_a)
#         return self.field_a


# class HelloOperator(BaseOperator):
#     template_fields = ("field_a", "field_b")
#
#     def __init__(self, field_a: str, field_b: str, **kwargs) -> None:
#         super().__init__(**kwargs)
#         self.field_b = field_b
#
#     def execute(self, context):
#         print(self.field_b)
#         return self.field_b


# class HelloOperator(BaseOperator):
#     template_fields = "field_a"
# 
#     def __init__(self, field_a: str, **kwargs) -> None:
#         super().__init__(**kwargs)
#         self.field_a = field_a
# 
#     def execute(self, context):
#         print(self.field_a)
#         return self.field_a
# 
# 
# class MyHelloOperator(HelloOperator):
#     template_fields = ("field_a", "field_b")
# 
#     def __init__(self, field_b: str, **kwargs) -> None:  # <- should be def __init__(field_a, field_b, **kwargs)
#         super().__init__(**kwargs)  # <- should be super().__init__(field_a=field_a, **kwargs)
#         self.field_b = field_b
# 
#     def execute(self, context):
#         print(self.field_b)
#         return self.field_b


# class HelloOperator(BaseOperator):
#     template_fields = "field_a"
#
#     def __init__(self, field_a: str, **kwargs) -> None:
#         super().__init__(**kwargs)
#         self.field_a = field_a.lower()  # <- assignment should be only self.field_a = field_a
#
#     def execute(self, context):
#         print(self.field_a)
#         return self.field_a


# class HelloOperator(BaseOperator):
#     template_fields: Sequence[str] = ("name",)
#
#     def __init__(self, name: str, world: str, **kwargs) -> None:
#         super().__init__(**kwargs)
#         self.name = name
#         self.world = world
#
#     def execute(self, context):
#         message = f"Hello {self.world} it's {self.name}!"
#         print(message)
#         return message
#
# class MyHelloOperator(HelloOperator):
#     template_fields: Sequence[str] = (*HelloOperator.template_fields, "world")



class HelloOperator(BaseOperator):
    template_fields = "field_a"

    def __init__(self, field_a: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.field_a = field_a

    def execute(self, context: Context) -> Any:
        print(self.field_a)
        return self.field_a


class MyHelloOperator(HelloOperator):
    template_fields = "field_a"
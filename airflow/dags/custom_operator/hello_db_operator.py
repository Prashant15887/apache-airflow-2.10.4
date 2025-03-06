from airflow.models.baseoperator import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.models import Variable

class HelloDBOperator(BaseOperator):
    # def __init__(self, name: str, postgres_conn_id: str, database: str, **kwargs) -> None:
    def __init__(self, name: str, postgres_conn_id: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        # self.mysql_conn_id = mysql_conn_id
        self.postgres_conn_id = postgres_conn_id
        # self.database = database

    def execute(self, context):
        # hook = MySqlHook(mysql_conn_id=self.mysql_conn_id, schema=self.database)
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        # hook = PostgresHook(Variable.get("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", default_var=None))
        sql = "select username from ab_user"
        result = hook.get_first(sql)
        message = f"Hello {result[0]}"
        print(message)
        return message
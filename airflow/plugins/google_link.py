from airflow.models.baseoperator import BaseOperator
from airflow.models.baseoperatorlink import BaseOperatorLink
from airflow.models.taskinstancekey import TaskInstanceKey
from airflow.plugins_manager import AirflowPlugin

class GoogleLink(BaseOperatorLink):
    name = "Google"

    def get_link(self, operator: BaseOperator, *, ti_key: TaskInstanceKey):
        return "https://www.google.com"


# Defining the plugin class
class AirflowExtraLinkPlugin(AirflowPlugin):
    name = "extra_link_plugin"
    operator_extra_links = [
        GoogleLink(),
    ]
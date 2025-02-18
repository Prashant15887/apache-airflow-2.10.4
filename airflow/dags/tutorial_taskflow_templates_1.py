
import pendulum

from airflow.decorators import dag, task
from airflow.operators.python import get_current_context
@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["example"],
    params={"foobar": "param_from_dag", "other_param": "from_dag"},
)
def tutorial_taskflow_templates_1():
    """
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the templates in the TaskFlow API.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    """
    @task(
        # Causes variables that end with `.sql` to be read and templates
        # within to be rendered.
        templates_exts=[".sql"],
    )
    def template_test(sql, test_var, data_interval_end):
        context = get_current_context()

        # Will print...
        # select * from test_data
        # where 1=1
        #     and run_id = 'scheduled__2024-10-09T00:00:00+00:00'
        #     and something_else = 'param_from_task'
        print(f"sql: {sql}")

        # Will print `scheduled__2024-10-09T00:00:00+00:00`
        print(f"test_var: {test_var}")

        # Will print `2024-10-10 00:00:00+00:00`.
        # Note how we didn't pass this value when calling the task. Instead
        # it was passed by the decorator from the context
        print(f"data_interval_end: {data_interval_end}")

        # Will print...
        # run_id: scheduled__2024-10-09T00:00:00+00:00; params.other_param: from_dag
        template_str = "run_id: {{ run_id }}; params.other_param: {{ params.other_param }}"
        rendered_template = context["task"].render_template(
            template_str,
            context,
        )
        print(f"rendered template: {rendered_template}")

        # Will print the full context dict
        print(f"context: {context}")
    template_test.override(
        # Will be merged with the dict defined in the dag
        # and override existing parameters.
        #
        # Must be passed into the decorator's parameters
        # through `.override()` not into the actual task
        # function
        params={"foobar": "param_from_task"},
    )(
        sql="sql/test.sql",
        test_var="{{ run_id }}",
    )
tutorial_taskflow_templates_1()

from datetime import datetime

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.providers.smtp.notifications.smtp import SmtpNotifier
#from myprovider.notifier import MyNotifier

with DAG(
    dag_id="example_notifier",
    start_date=datetime(2022, 1, 1),
    schedule_interval=None,
    #on_success_callback=MyNotifier(message="Success!"),
    #on_failure_callback=MyNotifier(message="Failure!"),
):
    t1 = BashOperator(
        task_id="example_task",
        bash_command="exit 1",
        #on_success_callback=MyNotifier(message="Task Succeeded!"),
        on_failure_callback=SmtpNotifier(
            from_email="singhprashant87@gmail.com",
            to="singhprashant87@gmail.com",
            subject="Task {{ ti.task_id }} failed",
        )
    )

    t2 = EmptyOperator(
        task_id="task",
        on_success_callback=SmtpNotifier(
            from_email="singhprashant87@gmail.com",
            to="singhprashant87@gmail.com",
            subject="Task {{ ti.task_id }} succeeded",
        ),
    )

    t2 >> t1
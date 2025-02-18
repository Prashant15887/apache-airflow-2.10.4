from airflow.decorators import task

@task
def add_task(x, y):
    print(f"Task args: x={x}, y={y}")
    return x + y
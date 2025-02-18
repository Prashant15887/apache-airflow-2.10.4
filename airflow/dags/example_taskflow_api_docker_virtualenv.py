from airflow.decorators import task

@task.docker(image="python:3.9-slim-bookworm", multiple_outputs=True)
def transform(order_data_dict: dict):
    """
    #### Transform task
    A simple Transform task which takes in the collection of order data and
    computes the total order value.
    """
    total_order_value = 0

    for value in order_data_dict.values():
        total_order_value += value

    return {"total_order_value": total_order_value}

from airflow.operators.python import ExternalPythonOperator

def callable_external_python():
    """
    Example function that will be performed in a virtual environment.

    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    import sys
    from time import sleep

    print(f"Running task via {sys.executable}")
    print("Sleeping")
    for _ in range(4):
        print("Please wait...", flush=True)
        sleep(1)
    print("Finished")

external_python_task = ExternalPythonOperator(
    task_id="external_python",
    python_callable=callable_external_python,
    python="/Library/Frameworks/Python.framework/Versions/3.12/bin",
)
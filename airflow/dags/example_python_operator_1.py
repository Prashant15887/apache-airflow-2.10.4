from airflow.operators.python import PythonVirtualenvOperator

def callable_virtualenv():
    """
    Example function that will be performed in a virtual environment.

    Importing at the module level ensures that it will not attempt to import the
    library before it is installed.
    """
    from time import sleep

    from colorama import Back, Fore, Style

    print(Fore.RED + "some red text")
    print(Back.GREEN + "and with a green background")
    print(Style.DIM + "and in dim text")
    print(Style.RESET_ALL)
    for _ in range(4):
        print(Style.DIM + "Please wait...", flush=True)
        sleep(1)
    print("Finished")

virtualenv_task = PythonVirtualenvOperator(
    task_id="virtualenv_python",
    python_callable=callable_virtualenv,
    requirements=["colorama==0.4.0"],
    system_site_packages=False,
)
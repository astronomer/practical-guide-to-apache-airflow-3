"""
Listing 1.1: Code structure of the task-oriented approach (PythonOperator version)

Simple dag with 3 tasks using the task-oriented approach.
This dag has the same structure as listing_1_1.py, but it uses PythonOperator instead of the task decorator,
the with DAG() context manager instead of the @dag decorator, and bitshift operators instead of the chain function.
Which syntax you choose is a matter of preference.
"""

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
)


def _extract():
    pass  # Any Python code to extract data from any tool


def _transform():
    pass  # Any Python code to transform data


def _load():
    pass  # Any Python code to load data to any tool


with DAG(
    dag_id="my_etl_pipeline"
):  # Define a DAG context, note that you need to pass the dag_id explicitly
    extract = PythonOperator(
        task_id="extract", python_callable=_extract
    )  # instantiate a PythonOperator that runs the _extract function
    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform,
    )
    load = PythonOperator(task_id="load", python_callable=_load)

    extract >> transform >> load  # Declare task dependencies using bitshift operators

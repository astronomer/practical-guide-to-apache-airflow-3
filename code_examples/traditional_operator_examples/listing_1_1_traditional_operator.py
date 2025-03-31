"""
Listing 1.1: Code structure of the task-oriented approach (PythonOperator version)

Simple dag with 3 tasks using the task-oriented approach.
This dag has the same structure as listing_1_1.py, but it uses PythonOperator instead of the task decorator,
the with DAG() context manager instead of the @dag decorator, and bitshift operators instead of the chain function.
"""

# TODO: add sample functions

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
)


with DAG(dag_id="my_etl_pipeline", schedule=None):
    extract = PythonOperator(
        task_id="extract", python_callable=lambda: None
    )
    transform = PythonOperator(
        task_id="transform",
        python_callable=lambda: None,
    )
    load = PythonOperator(
        task_id="load", python_callable=lambda: None
    )

    extract >> transform >> load

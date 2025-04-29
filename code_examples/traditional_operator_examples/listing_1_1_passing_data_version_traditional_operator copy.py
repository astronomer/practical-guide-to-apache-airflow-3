"""
Listing 1.1: Code structure of the task-oriented approach (PythonOperator version, passing data between tasks)

Simple dag with 3 tasks using the task-oriented approach.
This dag has the same structure as listing_1_1_passing_data_version.py, but it uses PythonOperator instead of the task decorator,
the with DAG() context manager instead of the @dag decorator, and bitshift operators instead of the chain function.
Which syntax you use is a matter of preference.
"""

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import (
    PythonOperator,
)


def _extract():
    return {"a": 23, "b": 19}  # Return data from the task


def _transform(my_data):
    summed_data = my_data["a"] + my_data["b"]  # Transform data
    return summed_data  # Return data from the task


def _load(my_sum):
    return f"Sum of a and b is {my_sum}"  # Print the sum to the logs


with DAG(dag_id="my_etl_pipeline", schedule=None):
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    transform = PythonOperator(
        task_id="transform",
        python_callable=_transform,
        op_kwargs={
            "my_data": extract.output
        },  # you can pass the returned .output of the previous task
    )
    load = PythonOperator(
        task_id="load",
        python_callable=_load,
        op_kwargs={
            "my_sum": "{{ ti.xcom_pull(task_ids='transform') }}"
        },  # you can also use a Jinja template to fetch any XCom value
    )

    extract >> transform >> load

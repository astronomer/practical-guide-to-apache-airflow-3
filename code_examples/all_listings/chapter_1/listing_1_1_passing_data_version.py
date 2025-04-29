"""
Listing 1.1: Code structure of the task-oriented approach (Alternative version)

This dag has the same structure as listing_1_1.py, but it passes
data between tasks.
"""

from airflow.sdk import dag, task


@dag  # Define a dag with default parameters
def my_etl_pipeline():  # By default, the @dag decorated function’s name is the id of the dag
    @task  # Add tasks in the dag with @task
    def extract() -> (
        dict
    ):  # By default, the @task decorated function name is the id of the task
        return {"a": 23, "b": 19}  # Return data from the task

    @task
    def transform(my_data: dict):
        summed_data = my_data["a"] + my_data["b"]  # Transform data
        return summed_data  # Return data from the task

    @task
    def load(my_sum: int):
        return f"Sum of a and b is {my_sum}"  # Print the sum to the logs

    _extract = extract()  # Call the extract task and assign it to a variable
    _transform = transform(
        my_data=_extract
    )  # Call the transform task with the output of the extract task as input
    load(
        my_sum=_transform
    )  # Call the load task with the output of the transform task as input


my_etl_pipeline()  # Call the dag function

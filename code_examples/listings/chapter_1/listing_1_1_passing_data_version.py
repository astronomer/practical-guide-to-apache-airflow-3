"""
Listing 1.1: Code structure of the task-oriented approach (Alternative version)

This dag has the same structure as listing_1_1.py, but it passes
data between tasks.
"""

from airflow.sdk import dag, task


@dag  # A
def my_etl_pipeline():  # B
    @task  # C
    def extract() -> dict:  # D
        return {"a": 23, "b": 19}  # E

    @task
    def transform(my_data: dict):
        summed_data = my_data["a"] + my_data["b"]  # F
        return summed_data  # G

    @task
    def load(my_sum: int):
        return f"Sum of a and b is {my_sum}"  # H

    _extract = extract()  # I
    _transform = transform(my_data=_extract)  # J
    load(my_sum=_transform)  # K


my_etl_pipeline()  # L


# A Define a dag with default parameters
# B By default, the @dag decorated function’s name is the id of the dag
# C Add tasks in the dag with @task
# D By default, the @task decorated function name is the id of the task
# E Return data from the task
# F Transform data
# G Return data from the task
# H Print the sum to the logs
# I Call the extract task and assign it to a variable
# J Call the transform task with the output of the extract task as input
# K Call the load task with the output of the transform task as input
# Note that Airflow automatically infers the dependencies between these tasks due to the data passing
# L Call the dag function

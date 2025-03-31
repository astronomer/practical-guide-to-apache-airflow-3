"""
Listing 1.1: Code structure of the task-oriented approach

Simple dag with 3 tasks using the task-oriented approach.
The tasks do not pass data between them, see
listing_1_1_passing_data_version.py for an example of passing data between tasks.
"""

from airflow.sdk import dag, task, chain


@dag  # A
def my_etl_pipeline():  # B
    @task  # C
    def extract():  # D
        pass  # E

    @task
    def transform():
        pass  # F

    @task
    def load():
        pass  # G

    chain(extract(), transform(), load())  # H


my_etl_pipeline()  # I


# A Define a dag with default parameters
# B By default, the @dag decorated function’s name is the id of the dag
# C Add tasks in the dag with @task
# D By default, the @task decorated function name is the id of the task
# E Any Python code to extract data from any tool
# F Any Python code to transform data
# G Any Python code to load data to any tool
# H Declare task dependencies (see chapter 2.4.2)
# I Call the dag function

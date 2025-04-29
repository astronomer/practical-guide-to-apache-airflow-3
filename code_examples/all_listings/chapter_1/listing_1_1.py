"""
Listing 1.1: Code structure of the task-oriented approach

Simple dag with 3 tasks using the task-oriented approach.
The tasks do not pass data between them, see
listing_1_1_passing_data_version.py for an example of passing data between tasks.
"""

from airflow.sdk import dag, task, chain


@dag  # Define a dag with default parameters
def my_etl_pipeline():  
    @task  # Add tasks in the dag with @task
    def extract():  
        pass  # Any Python code to extract data from any tool

    @task
    def transform():
        pass  # Any Python code to transform data

    @task
    def load():
        pass  # Any Python code to load data to any tool

    chain(extract(), transform(), load())  # Declare task dependencies (see chapter 2.4.2)


my_etl_pipeline()  # Call the dag function

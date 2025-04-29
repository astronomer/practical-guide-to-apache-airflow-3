from pendulum import datetime, duration
from airflow.sdk import dag, Asset


@dag(
    start_date=datetime(2025, 3, 1),
    schedule=[Asset("formatted_newsletter")],
    default_args={
        "retries": 2,
        "retry_delay": duration(minutes=3)
    }
)
def my_dag():
    pass
from airflow.sdk import Asset, dag
from pendulum import datetime

@dag(
    start_date=datetime(2025, 1, 1),
    schedule=(
        Asset("a") & (
            Asset("b") | Asset("c")
        )
    )
)
def my_dag():
    pass
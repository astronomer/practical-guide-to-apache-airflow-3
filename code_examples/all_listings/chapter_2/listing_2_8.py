from pendulum import datetime
from airflow.sdk import Asset, dag, task


@dag(
    start_date=datetime(2025, 3, 1),
    schedule=[Asset("formatted_newsletter")],
)
def personalize_newsletter():
    pass


personalize_newsletter()

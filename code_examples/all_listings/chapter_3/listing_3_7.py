from airflow.sdk import Asset, dag
from airflow.timetables.assets import AssetOrTimeSchedule
from airflow.timetables.trigger import CronTriggerTimetable
from pendulum import datetime


@dag(
    start_date=datetime(2025, 1, 1),
    schedule=AssetOrTimeSchedule(
        timetable=CronTriggerTimetable("0 0 * * *", timezone="UTC"),
        assets=(Asset("a") & Asset("b")),
    ),
)
def my_dag():
    pass

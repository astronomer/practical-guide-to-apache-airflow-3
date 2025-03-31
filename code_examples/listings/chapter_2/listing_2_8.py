from pendulum import datetime

from airflow.sdk import Asset, dag, task  # A


@dag(
    start_date=datetime(2025, 3, 1),  # B
    schedule=Asset("formatted_newsletter"),  # C
)
def personalize_newsletter():
    pass  # D


personalize_newsletter()  # E

# A Import of the dag and task decorators, and the Asset class
# B The first date on which the dag can run
# C Run as soon as the formatted_newsletter asset receives an update
# D pass will be replaced with task definitions in the next sections
# E Don’t forget to call the @dag decorated function

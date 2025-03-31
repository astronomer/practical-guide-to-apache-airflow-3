import os

from airflow.sdk import Asset, dag, task
from pendulum import datetime

OBJECT_STORAGE_SYSTEM = os.getenv(
    "OBJECT_STORAGE_SYSTEM", default="file"
)
OBJECT_STORAGE_CONN_ID = os.getenv(
    "OBJECT_STORAGE_CONN_ID", default=None
)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv(
    "OBJECT_STORAGE_PATH_NEWSLETTER",
    default="include/newsletter",
)
OBJECT_STORAGE_PATH_USER_INFO = os.getenv(
    "OBJECT_STORAGE_PATH_USER_INFO",
    default="include/user_data",
)  # A


@dag(
    start_date=datetime(2025, 3, 1),
    schedule=Asset("formatted_newsletter"),
)
def personalize_newsletter_version_1():
    @task  # B
    def get_user_info():  # C
        import json

        from airflow.io.path import ObjectStoragePath

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://"
            f"{OBJECT_STORAGE_PATH_USER_INFO}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )  # D

        user_info = []
        for file in object_storage_path.iterdir():
            bytes = file.read_block(
                offset=0, length=None
            )  # E
            user_info.append(json.loads(bytes))

        return user_info  # F

    _get_user_info = get_user_info()  # G


# A Fetch environment variables about the object storage used
# B The @task decorator can turn any Python function into an Airflow task
# C By default the task id is the name (get_user_info)  of the decorated function
# D Use the Airflow Object Storage abstraction as in listing 2.5
# E Read user info from json files
# F Push list of user info dictionaries to XCom
# G It is important to call the task function within the dag function to attach it to the dag,
# the output of the function can be assigned to a variable to be passed as input to downstream tasks

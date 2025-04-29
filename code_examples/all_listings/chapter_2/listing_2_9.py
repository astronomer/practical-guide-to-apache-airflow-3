import os
from airflow.sdk import Asset, dag, task
from pendulum import datetime

OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv(
    "OBJECT_STORAGE_PATH_NEWSLETTER",
    default="include/newsletter",
)
OBJECT_STORAGE_PATH_USER_INFO = os.getenv(
    "OBJECT_STORAGE_PATH_USER_INFO",
    default="include/user_data",
)


@dag(
    start_date=datetime(2025, 3, 1),
    schedule=[Asset("formatted_newsletter")],
)
def personalize_newsletter():
    @task
    def get_user_info() -> list[dict]:
        import json
        from airflow.sdk import ObjectStoragePath

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://" f"{OBJECT_STORAGE_PATH_USER_INFO}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )
        user_info = []
        for file in object_storage_path.iterdir():
            if file.is_file() and file.suffix == ".json":
                bytes = file.read_block(offset=0, length=None)
                user_info.append(json.loads(bytes))
        return user_info

    _get_user_info = get_user_info()

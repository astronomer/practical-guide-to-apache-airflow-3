import os

from airflow.sdk import Asset, dag
from airflow.decorators import task
from pendulum import datetime

_WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude={lat}&longitude={long}&current="
    "temperature_2m,relative_humidity_2m,"
    "apparent_temperature"
)

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


def _get_lat_long(location):
    import time

    from geopy.geocoders import Nominatim

    time.sleep(2)
    geolocator = Nominatim(
        user_agent="my-newsletter-app-2"
    )

    location = geolocator.geocode(location)

    return (
        float(location.latitude),
        float(location.longitude),
    )


from pendulum import datetime, duration
from airflow.sdk import dag


@dag(
    start_date=datetime(2025, 3, 1),
    schedule="@daily",
    default_args={
        "retries": 2,
        "retry_delay": duration(minutes=3),
    },  # A  # B  # C
)
def personalize_newsletter():
    @task
    def get_user_info() -> list[dict]:
        import json

        from airflow.io.path import ObjectStoragePath

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://"
            f"{OBJECT_STORAGE_PATH_USER_INFO}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )  # C

        user_info = []
        for file in object_storage_path.iterdir():
            bytes = file.read_block(
                offset=0, length=None
            )  # D
            user_info.append(json.loads(bytes))

        return user_info  # E

    _get_user_info = get_user_info()  # F

    @task(max_active_tis_per_dag=1, retries=4)  # A
    def get_weather_info(user: dict) -> dict:  # B
        import requests

        lat, long = _get_lat_long(user["location"])  # C
        r = requests.get(
            _WEATHER_URL.format(lat=lat, long=long)
        )
        user["weather"] = r.json()

        return user  # D

    _get_weather_info = get_weather_info.expand(
        user=_get_user_info
    )  # D

    @task(outlets=[Asset("personalized_newsletters")])
    def create_personalized_newsletter(
        user: dict,
        **context: dict,
    ) -> None:
        from airflow.io.path import ObjectStoragePath

        date = context["dag_run"].run_after.strftime(
            "%Y-%m-%d"
        )

        id = user["id"]
        name = user["name"]
        location = user["location"]
        actual_temp = user["weather"]["current"][
            "temperature_2m"
        ]
        apparent_temp = user["weather"]["current"][
            "apparent_temperature"
        ]
        rel_humidity = user["weather"]["current"][
            "relative_humidity_2m"
        ]

        new_greeting = (
            f"Hi {name}! \n\nIf you venture outside right now "
            f"in {location}, you'll find the temperature to be "
            f"{actual_temp}°C, but it will feel more like "
            f"{apparent_temp}°C. The relative humidity is "
            f"{rel_humidity}%."
        )

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://"
            f"{OBJECT_STORAGE_PATH_NEWSLETTER}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )

        daily_newsletter_path = (
            object_storage_path
            / f"{date}_newsletter.txt"
        )

        generic_content = (
            daily_newsletter_path.read_text()
        )

        personalized_content = generic_content.replace(
            "Hello Cosmic Traveler,",
            new_greeting,
        )

        personalized_newsletter_path = (
            object_storage_path
            / f"{date}_newsletter_userid_{id}.txt"
        )

        personalized_newsletter_path.write_text(
            personalized_content
        )

    create_personalized_newsletter.expand(
        user=_get_weather_info
    )


personalize_newsletter()

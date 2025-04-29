import os

from airflow.sdk import Asset, dag, task
from pendulum import datetime

_WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude={lat}&longitude={long}&current="
    "temperature_2m,relative_humidity_2m,"
    "apparent_temperature"
)
 
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


def _get_lat_long(location):
    import time

    from geopy.geocoders import Nominatim

    time.sleep(2)
    geolocator = Nominatim(user_agent="my-newsletter-app-2")

    location = geolocator.geocode(location)

    return (
        float(location.latitude),
        float(location.longitude),
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

    @task
    def get_weather_info(
        users: list[dict],
    ) -> list[dict]:
        import requests

        user_info_plus_weather = []

        for user in users:
            lat, long = _get_lat_long(user["location"])
            r = requests.get(_WEATHER_URL.format(lat=lat, long=long))
            user["weather"] = r.json()
            user_info_plus_weather.append(user)

        return user_info_plus_weather

    _get_weather_info = get_weather_info(users=_get_user_info)

    @task(outlets=[Asset("personalized_newsletters")])
    def create_personalized_newsletter(
        user_info_plus_weather: list[dict],
        **context: dict,
    ) -> None:
        from airflow.sdk import ObjectStoragePath

        date = context["dag_run"].run_after.strftime("%Y-%m-%d")

        for user in user_info_plus_weather:
            id = user["id"]
            name = user["name"]
            location = user["location"]
            actual_temp = user["weather"]["current"]["temperature_2m"]
            apparent_temp = user["weather"]["current"]["apparent_temperature"]
            rel_humidity = user["weather"]["current"]["relative_humidity_2m"]

            new_greeting = (
                f"Hi {name}! \n\nIf you venture outside right now "
                f"in {location}, you'll find the temperature to be "
                f"{actual_temp}°C, but it will feel more like "
                f"{apparent_temp}°C. The relative humidity is "
                f"{rel_humidity}%."
            )

            object_storage_path = ObjectStoragePath(
                f"{OBJECT_STORAGE_SYSTEM}://" f"{OBJECT_STORAGE_PATH_NEWSLETTER}",
                conn_id=OBJECT_STORAGE_CONN_ID,
            )

            daily_newsletter_path = object_storage_path / f"{date}_newsletter.txt"

            generic_content = daily_newsletter_path.read_text()

            personalized_content = generic_content.replace(
                "Hello Cosmic Traveler,",
                new_greeting,
            )

            personalized_newsletter_path = (
                object_storage_path / f"{date}_newsletter_userid_{id}.txt"
            )

            personalized_newsletter_path.write_text(personalized_content)

    create_personalized_newsletter(user_info_plus_weather=_get_weather_info)


personalize_newsletter()

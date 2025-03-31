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
    def get_weather_info(user: dict):  # B
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

    @task(max_active_tis_per_dag=16)
    def create_personalized_quote(user, **context):
        from openai import OpenAI

        import os
        import re

        client = OpenAI(
            api_key=os.getenv("OPENAI_API_KEY")
        )
        id = user["id"]
        name = user["name"]
        motivation = user["motivation"]
        favorite_sci_fi_character = user[
            "favorite_sci_fi_character"
        ]

        series = favorite_sci_fi_character.split(" (")[
            1
        ].replace(")", "")
        date = context["execution_date"].strftime(
            "%Y-%m-%d"
        )

        with open(
            f"{_PATH_TO_NEWSLETTER_FOLDER}/{date}_newsletter.txt",
            "r",
        ) as f:
            newsletter_content = f.read()

        quotes = re.findall(
            r'\d+\.\s+"([^"]+)"', newsletter_content
        )

        system_prompt = (
            f"You are {favorite_sci_fi_character} giving advice to your best friend {name}. "
            f"{name} once said '{motivation}' and today they are especially in need of some encouragement. "
            f"Please write a personalized quote for them based on the historic quotes provided, include "
            f"an insider reference to {series} that only someone who has seen it would understand. "
            "Do NOT include the series name in the quote. Do NOT verbatim repeat any of the provided quotes. "
            "The quote should be between 200 and 500 characters long."
        )
        user_prompt = (
            "The quotes to modify are:\n"
            + "\n".join(quotes)
        )

        completion = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": system_prompt,
                },
                {
                    "role": "user",
                    "content": user_prompt,
                },
            ],
        )

        # Extract the generated response from the API
        generated_response = completion.choices[
            0
        ].message.content

        # get the current context and define the custom map index variable
        from airflow.operators.python import (
            get_current_context,
        )

        context = get_current_context()

        context["my_custom_map_index"] = (
            f"Personalized Quote for: {user['name']} (id: {user['id']}) in {user['location']}"
        )

        return generated_response

    _create_personalized_quote = (
        create_personalized_quote.expand(
            user=_get_user_info
        )
    )

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
        personalized_info=_get_weather_info.zip(
            _create_personalized_quote
        )
    )


personalize_newsletter()

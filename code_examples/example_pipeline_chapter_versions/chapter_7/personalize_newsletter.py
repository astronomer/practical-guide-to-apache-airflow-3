import os

from airflow.decorators import task
from airflow.sdk import dag, Asset, AssetWatcher
from airflow.providers.common.messaging.triggers.msg_queue import MessageQueueTrigger
from pendulum import datetime, duration

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

SYSTEM_PROMPT = (
    "You are {favorite_sci_fi_character} "
    "giving advice to your best friend {name}. "
    "{name} once said '{motivation}' and today "
    "they are especially in need of some encouragement. "
    "Please write a personalized quote for them "
    "based on the historic quotes provided, include "
    "an insider reference to {series} that only someone "
    "who has seen it would understand. "
    "Do NOT include the series name in the quote. "
    "Do NOT verbatim repeat any of the provided quotes. "
    "The quote should be between 200 and 500 characters long."
)


def _get_lat_long(location):
    """
    Note that this version of the function caches the geocoding
    """
    import time
    from airflow.sdk import ObjectStoragePath
    from geopy.geocoders import Nominatim
    import json

    locations_file = ObjectStoragePath(
        f"{OBJECT_STORAGE_SYSTEM}://" f"{OBJECT_STORAGE_LOCATIONS_FILE}",
        conn_id=OBJECT_STORAGE_CONN_ID,
    )
    if not locations_file.exists():
        locations_file.write_text("{}")

    locations_data = json.loads(locations_file.read_text())

    if location in locations_data.keys():
        return tuple(locations_data[location])

    time.sleep(10)
    geolocator = Nominatim(user_agent="my-newsletter-app-5")

    location_object = geolocator.geocode(location)

    coordinates = (float(location_object.latitude), float(location_object.longitude))

    locations_data[location] = coordinates

    locations_file.write_text(json.dumps(locations_data))

    return coordinates


# Configure the SQS queue trigger

SQS_QUEUE_URL = os.getenv(
    "SQS_QUEUE_URL", default="https://sqs.region.amazonaws.com/account/queue"
)

trigger = MessageQueueTrigger(
    queue=SQS_QUEUE_URL, aws_conn_id="aws_default", waiter_delay=10
)
sqs_asset = Asset(
    "sqs_queue_asset", watchers=[AssetWatcher(name="sqs_watcher", trigger=trigger)]
)


@dag(
    start_date=datetime(2025, 3, 1),
    schedule=(
        Asset("formatted_newsletter") | sqs_asset
    ),  # Create a conditional schedule to run the dag when one of the assets is updated
    default_args={
        "retries": 2,
        "retry_delay": duration(minutes=3),
    },
    tags=["newsletter_pipeline"],
)
def personalize_newsletter():
    @task
    def get_user_info(
        **context,
    ) -> list[
        dict
    ]:  # This task is the only task that needs adaptation to work with the SQS queue trigger, the rest of the dag remains unchanged
        import json

        asset_event_name = None

        triggering_asset_events = context[
            "triggering_asset_events"
        ]  # fetch the triggering asset events
        for asset_event in triggering_asset_events[sqs_asset]:
            asset_event_name = (
                asset_event.asset.name
            )  # get the name of the triggering asset event
            asset_extra = (
                asset_event.extra
            )  # get the extra information from the triggering asset event (when triggered via SQS this contains the message from the queue)

        if (
            asset_event_name == "sqs_queue_asset"
        ):  # if the dag was triggered using event-driven scheduling only one newsletter is created: for the user named in the message
            print("Triggered by SQS queue asset...")
            print("Processing the message from the SQS queue: ", asset_extra)
            user_info = [
                json.loads(asset_extra["payload"]["message_batch"][0]["Body"])
            ]  # get the message from the SQS queue

        else:  # if the dag ran based on updates to another datasets i.e. on its regular batch cadence, all subscribers are fetched
            print("Triggered by formatted_newsletter asset...")
            print("Creating newsletters for all subscribers...")
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

    @task(max_active_tis_per_dag=1, retries=4, pool="weather")
    def get_weather_info(user: dict) -> dict:
        import requests

        lat, long = _get_lat_long(user["location"])
        r = requests.get(_WEATHER_URL.format(lat=lat, long=long))
        user["weather"] = r.json()

        return user

    _get_weather_info = get_weather_info.expand(user=_get_user_info)

    @task(max_active_tis_per_dag=16)
    def create_personalized_quote(
        system_prompt,
        user,
        **context,
    ):
        import re

        from airflow.sdk import ObjectStoragePath
        from airflow.providers.openai.hooks.openai import (
            OpenAIHook,
        )

        my_openai_hook = OpenAIHook(conn_id="my_openai_conn")
        client = my_openai_hook.get_conn()

        id = user["id"]
        name = user["name"]
        motivation = user["motivation"]
        favorite_sci_fi_character = user["favorite_sci_fi_character"]
        series = favorite_sci_fi_character.split(" (")[1].replace(")", "")
        date = context["dag_run"].run_after.strftime("%Y-%m-%d")

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )

        date_newsletter_path = object_storage_path / f"{date}_newsletter.txt"

        newsletter_content = date_newsletter_path.read_text()

        quotes = re.findall(r'\d+\.\s+"([^"]+)"', newsletter_content)

        print(quotes)

        system_prompt = system_prompt.format(
            favorite_sci_fi_character=favorite_sci_fi_character,
            motivation=motivation,
            name=name,
            series=series,
        )
        user_prompt = "The quotes to modify are:\n" + "\n".join(quotes)

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

        generated_response = completion.choices[0].message.content

        return {
            "user_id": id,
            "personalized_quote": generated_response,
        }

    _create_personalized_quote = create_personalized_quote.partial(
        system_prompt=SYSTEM_PROMPT
    ).expand(user=_get_user_info)

    @task
    def combine_information(
        user_info: list[dict],
        personalized_quotes: list[dict],
    ) -> None:
        user_info_dict = {user["id"]: user for user in user_info}
        for quote in personalized_quotes:
            user_id = quote["user_id"]
            user_info_dict[user_id]["personalized_quote"] = quote["personalized_quote"]

        return list(user_info_dict.values())

    _combine_information = combine_information(
        user_info=_get_weather_info,
        personalized_quotes=_create_personalized_quote,
    )

    @task
    def create_personalized_newsletter(
        user: list[dict],
        **context: dict,
    ) -> None:
        import textwrap

        from airflow.sdk import ObjectStoragePath

        date = context["dag_run"].run_after.strftime("%Y-%m-%d")

        id = user["id"]
        name = user["name"]
        location = user["location"]
        favorite_sci_fi_character = user["favorite_sci_fi_character"]
        character_name = favorite_sci_fi_character.split(" (")[0]
        actual_temp = user["weather"]["current"]["temperature_2m"]
        apparent_temp = user["weather"]["current"]["apparent_temperature"]
        rel_humidity = user["weather"]["current"]["relative_humidity_2m"]
        quote = user["personalized_quote"]
        wrapped_quote = textwrap.fill(quote, width=50)

        new_greeting = (
            f"Hi {name}! \n\nIf you venture outside right now in {location}, "
            f"you'll find the temperature to be {actual_temp}°C, but it will "
            f"feel more like {apparent_temp}°C. The relative humidity is "
            f"{rel_humidity}%."
        )

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://" f"{OBJECT_STORAGE_PATH_NEWSLETTER}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )

        daily_newsletter_path = object_storage_path / f"{date}_newsletter.txt"

        generic_content = daily_newsletter_path.read_text()

        updated_content = generic_content.replace(
            "Hello Cosmic Traveler,", new_greeting
        )

        personalized_quote = (
            f"\n-----------\n"
            f"This is what {character_name} might say to you today:\n\n"
            f"{wrapped_quote}\n\n"
            f"-----------"
        )

        updated_content = updated_content.replace(
            "Have a fantastic journey!",
            f"{personalized_quote}\n\nHave a fantastic journey!",
        )

        personalized_newsletter_path = (
            object_storage_path / f"{date}_newsletter_userid_{id}.txt"
        )

        personalized_newsletter_path.write_text(updated_content)

    create_personalized_newsletter.expand(user=_combine_information)


personalize_newsletter()

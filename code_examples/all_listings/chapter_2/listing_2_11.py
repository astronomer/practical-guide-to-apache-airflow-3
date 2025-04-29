from airflow.sdk import Asset, task

# ... Dag definition and first two tasks...

@task(outlets=[Asset("personalized_newsletters")])
def create_personalized_newsletter(
    user_info_plus_weather: list[dict], **context: dict
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

    create_personalized_newsletter(
        user_info_plus_weather=_get_weather_info,
    )

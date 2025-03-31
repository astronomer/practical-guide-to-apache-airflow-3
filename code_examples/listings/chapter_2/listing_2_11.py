@task(outlets=[Asset("personalized_newsletters")])  # A
def add_name_and_weather_to_newsletter(
    user_info_plus_weather, **context
):
    from airflow.io.path import ObjectStoragePath

    date = context["dag_run"].run_after.strftime(
        "%Y-%m-%d"
    )  # B

    for user in user_info_plus_weather:  # C
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
        )  # D

        object_storage_path = ObjectStoragePath(
            f"{OBJECT_STORAGE_SYSTEM}://"
            f"{OBJECT_STORAGE_PATH_NEWSLETTER}",
            conn_id=OBJECT_STORAGE_CONN_ID,
        )  # E

        daily_newsletter_path = (
            object_storage_path
            / f"{date}_newsletter.txt"
        )

        generic_content = (
            daily_newsletter_path.read_text()
        )  # F

        personalized_content = generic_content.replace(
            "Hello Cosmic Traveler,",
            new_greeting,
        )  # G

        personalized_newsletter_path = (
            object_storage_path
            / f"{date}_newsletter_userid_{id}.txt"
        )

        personalized_newsletter_path.write_text(
            personalized_content
        )  # H


add_name_and_weather_to_newsletter(
    user_info_plus_weather=_get_weather_info  # I
)
# A
# B Fetch the date of the dag run from the context, to match to the newsletter template of the day
# C Iterate through list with user info dictionaries
# D Use personalized information to create a new newsletter greeting
# E Get the path to the newsletter folder
# F Read the text from the newsletter template
# G Replace the generic with the personalized greeting
# H Write the personalized newsletter to a new file
# I Call the task function and provide the argument input to be pulled from XCom

_WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude={lat}&longitude={long}&current="
    "temperature_2m,relative_humidity_2m,"
    "apparent_temperature"
)  # A


def _get_lat_long(location):  # B
    import time

    from geopy.geocoders import Nominatim

    time.sleep(2)  # C
    geolocator = Nominatim(
        user_agent="practical-guide-to-airflow-3"
    )

    location = geolocator.geocode(location)  # D

    return (
        float(location.latitude),
        float(location.longitude),
    )

    # ... DAG definition and first task definition ...

    @task
    def get_weather_info(users: list[dict]):
        import requests

        user_info_plus_weather = []

        for user in users:  # E
            lat, long = _get_lat_long(
                user["location"]
            )  # F
            r = requests.get(
                _WEATHER_URL.format(lat=lat, long=long)
            )  # G
            user["weather"] = r.json()
            user_info_plus_weather.append(user)

        return user_info_plus_weather  # H

    _get_weather_info = get_weather_info(
        users=_get_user_info  # I
    )


# A The weather API URL with placeholders for the latitude and longitude values
# B Supporting function, typically defined in the include folder and imported
# C Delay to prevent rate limits using the free tier Nominatim API
# D Convert location string to lat/long
# E Loop through each dictionary in the users list
# F Call the supporting function
# G Call the Open Meteo API
# H Push the user and weather info to XCom
# I The get_weather_info task receives the user_info returned from the previous task via XCom.

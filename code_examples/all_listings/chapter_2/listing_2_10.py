from airflow.sdk import task

_WEATHER_URL = (
    "https://api.open-meteo.com/v1/forecast?"
    "latitude={lat}&longitude={long}&current="
    "temperature_2m,relative_humidity_2m,"
    "apparent_temperature"
)


def _get_lat_long(location):
    import time
    from geopy.geocoders import Nominatim

    time.sleep(2)
    geolocator = Nominatim(user_agent="MyApp/1.0 (my_email@example.com)")
    location = geolocator.geocode(location)
    return (
        float(location.latitude),
        float(location.longitude),
    )

# ... Dag definition and first task...

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

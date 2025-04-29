@task(max_active_tis_per_dag=1)
def get_weather_info(user: dict) -> dict:
    import requests

    lat, long = _get_lat_long(user["location"])
    r = requests.get(_WEATHER_URL.format(lat=lat, long=long))
    user["weather"] = r.json()
    return user


_get_weather_info = get_weather_info.expand(user=_get_user_info)

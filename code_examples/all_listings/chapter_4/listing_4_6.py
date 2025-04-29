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

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

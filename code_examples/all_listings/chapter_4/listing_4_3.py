@task
def my_genai_task():
    from airflow.providers.openai.hooks.openai import OpenAIHook

    my_openai_hook = OpenAIHook(conn_id="my_openai_conn")
    client = my_openai_hook.get_conn()

from airflow.sdk import task

@task(
    retries=4
)
def my_task():
    pass
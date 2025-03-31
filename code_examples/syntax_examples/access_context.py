from airflow.sdk import asset, dag
from airflow.decorators import task


@asset(schedule="@daily")
def access_context_asset(**context):
    print(context["context"])


@dag
def access_context_dag():
    @task
    def access_context_task(**context):
        print(context["logical_date"])

    access_context_task()


access_context_dag()

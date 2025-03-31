from airflow.sdk import dag
from airflow.decorators import task


@dag
def cross_dag_xcom_upstream():
    @task
    def push_to_xcom():
        return "Hello!"

    push_to_xcom()


cross_dag_xcom_upstream()


@dag
def cross_dag_xcom_downstream():
    @task
    def pull_from_xcom(**context):
        my_data = context["task_instance"].xcom_pull(
            dag_id="cross_dag_xcom_upstream",
            task_ids=["push_to_xcom"],
            key="return_value",
            include_prior_dates=True,
        )
        return my_data

    pull_from_xcom()


cross_dag_xcom_downstream()

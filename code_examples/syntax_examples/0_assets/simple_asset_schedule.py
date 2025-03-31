from airflow.sdk import dag, Asset
from airflow.decorators import task
from airflow.operators.bash_operator import BashOperator
from airflow.models.baseoperator import (
    chain,
    chain_linear,
    cross_downstream,
)


@dag(tags=["syntax_examples", "0_assets"])
def simple_asset_schedule_upstream():
    @task(outlets=[Asset("simple_asset_schedule")])
    def my_producer_task():
        pass

    t1 = my_producer_task()

    chain(t1)
    chain_linear(t1)
    cross_downstream(t1)


simple_asset_schedule_upstream()


@dag(
    schedule=[Asset("simple_asset_schedule")],
    tags=["syntax_examples", "0_assets"],
)
def simple_asset_schedule_downstream():
    @task
    def my_task():
        pass

    test = BashOperator(
        task_id="test", bash_command="echo test"
    )

    my_task()


simple_asset_schedule_downstream()

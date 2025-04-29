from airflow.sdk import dag, task, chain
from airflow.providers.standard.operators.empty import (
    EmptyOperator,
)


@dag
def dependency_syntax():
    @task
    def my_task_1():
        pass

    _my_task_1 = my_task_1()

    @task
    def my_task_2():
        pass

    _my_task_2 = my_task_2()

    my_task_3 = EmptyOperator(task_id="my_task_3")
    my_task_4 = EmptyOperator(task_id="my_task_4")

    chain(
        _my_task_1,
        [_my_task_2, my_task_3],
        my_task_4,
    )

    chain(_my_task_1, my_task_4)


dependency_syntax()

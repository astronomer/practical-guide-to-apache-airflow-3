from airflow.decorators import dag, task


@dag
def simple_dynamic_task_mapping():
    @task
    def get_nums() -> list[int]:
        import random

        return [
            random.randint(1, 10)
            for _ in range(random.randint(3, 7))
        ]  # A

    @task
    def times_a(a: int, num: int) -> int:  # B
        return a * num

    @task
    def add_b(b: int, num: int) -> int:  # C
        return b + num

    _get_nums = get_nums()  # D

    _times_a = times_a.partial(
        a=2  # E
    ).expand(
        num=_get_nums  # F
    )

    add_b.partial(
        b=10  # G
    ).expand(
        num=_times_a  # H
    )


simple_dynamic_task_mapping()

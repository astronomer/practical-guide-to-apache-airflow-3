from airflow.sdk import dag, task


@dag
def simple_dynamic_task_mapping():
    @task
    def get_nums() -> list[int]:
        import random

        return [random.randint(1, 10) for _ in range(random.randint(3, 7))]

    @task
    def times_a(a: int, num: int) -> int:
        return a * num

    @task
    def add_b(b: int, num: int) -> int:
        return b + num

    _get_nums = get_nums()
    _times_a = times_a.partial(a=2).expand(num=_get_nums)
    add_b.partial(b=10).expand(num=_times_a)


simple_dynamic_task_mapping()

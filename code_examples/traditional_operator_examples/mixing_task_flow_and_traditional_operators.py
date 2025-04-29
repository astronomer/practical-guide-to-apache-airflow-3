from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator


@dag
def mixing_task_flow_and_traditional_operators():

    # ---------------------------------------------- #
    # A Traditional to TaskFlow without data passing #
    # ---------------------------------------------- #

    @task
    def first_task_A():
        return "hello world"

    second_task_A = EmptyOperator(
        task_id="second_task_A",
    )

    first_task_A() >> second_task_A

    # ------------------------------------------- #
    # B Traditional to TaskFlow with data passing #
    # ------------------------------------------- #

    def _first_task_B():
        return "hello world"

    first_task_B = PythonOperator(task_id="first_task_B", python_callable=_first_task_B)

    first_task_B_result = first_task_B.output

    @task
    def second_task_B(first_task_B_result_value):
        return f"{first_task_B_result_value} and hello again"

    # Providing first_task_B_result (traditional operator result) as an argument to a second_task_B
    # (a TaskFlow function) automatically registers the dependency between the tasks.
    second_task_B(first_task_B_result)

    # ---------------------------------------------- #
    # C TaskFlow to Traditional without data passing #
    # ---------------------------------------------- #

    @task
    def first_task_C():
        return "hello world"

    second_task_C = EmptyOperator(task_id="second_task_C")

    first_task_C() >> second_task_C

    # ------------------------------------------- #
    # D TaskFlow to Traditional with data passing #
    # ------------------------------------------- #

    @task
    def first_task_D():
        return "hello"

    first_task_D_result = first_task_D()

    def _second_task_callable(x):
        reversed_text = x[::-1]
        return f"Reversed version of prior task result: {reversed_text}"

    # when first_task_D_result (an XComArg) is provided as an argument to a templated function (op_args)
    # Airflow automatically registers that second_task depends on first_task_D
    PythonOperator(
        task_id="second_task_D",
        python_callable=_second_task_callable,
        op_args=[first_task_D_result],  # note op_args requires a LIST of XComArgs
    )


mixing_task_flow_and_traditional_operators()

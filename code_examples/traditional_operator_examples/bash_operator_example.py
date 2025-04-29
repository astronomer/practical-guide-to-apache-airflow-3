from airflow.sdk import dag
from airflow.providers.standard.operators.bash import BashOperator


@dag
def bash_operator_example():

    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello, World!'",
    )

    say_hello_with_params = BashOperator(
        task_id="say_hello_with_args",
        bash_command="echo 'Hello, {{ params.name }}!'",
        params={"name": "Airflow"},
    )

    say_hello_with_env = BashOperator(
        task_id="say_hello_with_env",
        bash_command="echo $NAME",
        env={"NAME": "Hello Airflow!"},
    )

    (
        say_hello
        >> say_hello_with_params
        >> say_hello_with_env
    )


bash_operator_example()

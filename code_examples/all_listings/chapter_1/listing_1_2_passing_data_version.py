"""
Listing 1.2: Code structure of the asset-oriented approach (Alternative version)

This pipeline has the same structure as listing_1_2.py, but it passes
data between assets.
"""

from airflow.sdk import asset


@asset(schedule="@daily")
def extracted_data():
    return {"a": 23, "b": 19}  # Return data from the asset


@asset(schedule=extracted_data)
def transformed_data(
    context,
):  # pass in the contex, note that you do not need to use **
    my_data = context["ti"].xcom_pull(
        dag_id="extracted_data",  # The dag_id is the name of the asset
        task_ids="extracted_data",  # The task_id is the name of the asset
        key="return_value",  # The key for returned data is "return_value"
        include_prior_dates=True,  # Include prior dates to pull data from previous runs
    )  # Pull data from the previous asset using a cross-dag xcom pull

    summed_data = my_data["a"] + my_data["b"]  # Transform data
    return summed_data


@asset(schedule=transformed_data)
def loaded_data(context):  # pass in the context, note that you do not need to use **
    my_sum = context["ti"].xcom_pull(
        dag_id="transformed_data",
        task_ids="transformed_data",
        key="return_value",
        include_prior_dates=True,
    )  # Pull data from the previous asset using a cross-dag xcom pull

    return f"Sum of a and b is {my_sum}"

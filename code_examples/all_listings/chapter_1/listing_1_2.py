"""
Listing 1.2: Code structure of the asset-oriented approach

Simple ETL pipeline created using 3 assets.
Each asset defines one dag with one task that materializes the asset.
The assets do not pass data between them, see listing_1_2_passing_data_version.py
for an example of passing data between assets.
"""

from airflow.sdk import asset


@asset(schedule="@daily")  # Define an asset that runs daily
def extracted_data():  # By default the @asset decorated function’s name is the name of the asset
    pass  # Any Python code to extract data from any tool


@asset(schedule=extracted_data)  # Schedule to materialize as soon as the upstream asset received an update
def transformed_data():
    pass  # Any Python code to transform data


@asset(schedule=transformed_data)
def loaded_data():
    pass  # Any Python code to load data to any tool

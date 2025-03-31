"""
Listing 1.2: Code structure of the asset-oriented approach

This pipeline has the same structure as listing_1_2.py, but it passes
data between assets.
"""

# TODO add data passing

from airflow.sdk import asset


@asset  # A
def extracted_data():  # B
    pass  # C


@asset(schedule=extracted_data)  # D
def transformed_data():
    pass  # E


@asset(schedule=transformed_data)
def loaded_data():
    pass  # F


# A Define an asset
# B By default the @asset decorated function’s name is the name of the asset
# C Any Python code to extract data from any tool
# D Schedule to materialize as soon as the upstream asset received an update
# E Any Python code to transform data
# F Any Python code to load data to any tool

from airflow.sdk import asset  # A


@asset(schedule="@daily")  # B
def raw_zen_quotes():  # C
    """
    Extracts a random set of quotes.
    """
    import requests  # D

    r = requests.get(
        "https://zenquotes.io/api/quotes/random"
    )  # E
    quotes = r.json()

    return quotes  # F


# A Import the asset decorator
# B Schedule the asset to materialize once a day at midnight UTC
# C By default the function name (raw_zen_quotes) is the name of the asset and dag
# D Modules imported within the function are only imported at runtime
# E Call to the Zen Quotes API
# F Data returned from the function is saved using the XCom feature

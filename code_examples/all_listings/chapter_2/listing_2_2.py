from airflow.sdk import asset  


@asset(schedule="@daily")  
def raw_zen_quotes():  
    """
    Extracts a random set of quotes.
    """
    import requests  

    r = requests.get(
        "https://zenquotes.io/api/quotes/random"
    )  
    quotes = r.json()

    return quotes  


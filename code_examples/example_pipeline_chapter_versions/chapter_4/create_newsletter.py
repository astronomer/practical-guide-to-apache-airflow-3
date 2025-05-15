from airflow.sdk import asset 


@asset(schedule="@daily")  
def raw_zen_quotes() -> list[dict]:  
    """
    Extracts a random set of quotes.
    """
    import requests  

    r = requests.get(
        "https://zenquotes.io/api/quotes/random"
    )  
    quotes = r.json()

    return quotes  


from airflow.sdk import asset


@asset(schedule=[raw_zen_quotes])  
def selected_quotes(context: dict) -> dict:  
    """
    Transforms the extracted raw_zen_quotes.
    """

    import numpy as np  # C

    raw_zen_quotes = context["ti"].xcom_pull(  
        dag_id="raw_zen_quotes",  
        task_ids=["raw_zen_quotes"],  
        key="return_value",  
        include_prior_dates=True,  
    )[0]  # [0] Added to account for a bugfix in version 3.0.1, see: https://github.com/apache/airflow/pull/49692  

    quotes_character_counts = [
        int(quote["c"]) for quote in raw_zen_quotes
    ]
    median = np.median(quotes_character_counts)

    median_quote = min(
        raw_zen_quotes,
        key=lambda quote: abs(int(quote["c"]) - median),
    )
    raw_zen_quotes.pop(
        raw_zen_quotes.index(median_quote)
    )
    short_quote = [
        quote
        for quote in raw_zen_quotes
        if int(quote["c"]) < median
    ][0]
    long_quote = [
        quote
        for quote in raw_zen_quotes
        if int(quote["c"]) > median
    ][0]

    return {
        "median_q": median_quote,
        "short_q": short_quote,
        "long_q": long_quote,
    }


import os  # A
from airflow.sdk import asset

OBJECT_STORAGE_SYSTEM = os.getenv(
    "OBJECT_STORAGE_SYSTEM", default="file"
)
OBJECT_STORAGE_CONN_ID = os.getenv(
    "OBJECT_STORAGE_CONN_ID", default=None
)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv(
    "OBJECT_STORAGE_PATH_NEWSLETTER",
    default="include/newsletter",
)  # B


@asset(
    schedule=[selected_quotes],
)
def formatted_newsletter(context: dict) -> None:  # D
    """
    Formats the newsletter.
    """
    from airflow.sdk import ObjectStoragePath  # E

    object_storage_path = ObjectStoragePath(
        f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",
        conn_id=OBJECT_STORAGE_CONN_ID,
    )  # F

    date = context["dag_run"].run_after.strftime(
        "%Y-%m-%d"
    )  # G

    print(context["dag_run"].run_after)

    selected_quotes = context["ti"].xcom_pull(
        dag_id="selected_quotes",
        task_ids=["selected_quotes"],
        key="return_value",
        include_prior_dates=True,
    )[0]  # [0] Added to account for a bugfix in version 3.0.1, see: https://github.com/apache/airflow/pull/49692

    newsletter_template_path = (
        object_storage_path / "newsletter_template.txt"
    )

    newsletter_template = (
        newsletter_template_path.read_text()
    )  # I

    newsletter = newsletter_template.format(
        quote_text_1=selected_quotes["short_q"]["q"],
        quote_author_1=selected_quotes["short_q"]["a"],
        quote_text_2=selected_quotes["median_q"]["q"],
        quote_author_2=selected_quotes["median_q"]["a"],
        quote_text_3=selected_quotes["long_q"]["q"],
        quote_author_3=selected_quotes["long_q"]["a"],
        date=date,
    )  # J

    date_newsletter_path = (
        object_storage_path / f"{date}_newsletter.txt"
    )

    date_newsletter_path.write_text(newsletter)  # K

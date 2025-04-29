from airflow.sdk import asset

@asset(schedule=[raw_zen_quotes])  
def selected_quotes(context: dict) -> dict:  
    """
    Transforms the extracted raw_zen_quotes.
    """
    import numpy as np

    raw_zen_quotes = context["ti"].xcom_pull(
        dag_id="raw_zen_quotes",
        task_ids=["raw_zen_quotes"],
        key="return_value",
        include_prior_dates=True,
    )
    quotes_character_counts = [int(quote["c"]) for quote in raw_zen_quotes]
    median = np.median(quotes_character_counts)
    median_quote = min(
        raw_zen_quotes,
        key=lambda quote: abs(int(quote["c"]) - median),
    )
    raw_zen_quotes.pop(raw_zen_quotes.index(median_quote))
    short_quote = [quote for quote in raw_zen_quotes if int(quote["c"]) < median][0]
    long_quote = [quote for quote in raw_zen_quotes if int(quote["c"]) > median][0]
    return {
        "median_q": median_quote,
        "short_q": short_quote,
        "long_q": long_quote,
    }


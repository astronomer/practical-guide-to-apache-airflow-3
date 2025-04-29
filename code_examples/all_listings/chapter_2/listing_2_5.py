import os
from airflow.sdk import asset

OBJECT_STORAGE_SYSTEM = os.getenv("OBJECT_STORAGE_SYSTEM", default="file")
OBJECT_STORAGE_CONN_ID = os.getenv("OBJECT_STORAGE_CONN_ID", default=None)
OBJECT_STORAGE_PATH_NEWSLETTER = os.getenv(
    "OBJECT_STORAGE_PATH_NEWSLETTER",
    default="include/newsletter",
)


@asset(schedule=[selected_quotes])
def formatted_newsletter(context: dict) -> None:
    """
    Formats the newsletter.
    """
    from airflow.sdk import ObjectStoragePath

    object_storage_path = ObjectStoragePath(
        f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",
        conn_id=OBJECT_STORAGE_CONN_ID,
    )
    date = context["dag_run"].run_after.strftime("%Y-%m-%d")
    selected_quotes = context["ti"].xcom_pull(
        dag_id="selected_quotes",
        task_ids=["selected_quotes"],
        key="return_value",
        include_prior_dates=True,
    )

    newsletter_template_path = object_storage_path / "newsletter_template.txt"
    newsletter_template = newsletter_template_path.read_text()
    newsletter = newsletter_template.format(
        quote_text_1=selected_quotes["short_q"]["q"],
        quote_author_1=selected_quotes["short_q"]["a"],
        quote_text_2=selected_quotes["median_q"]["q"],
        quote_author_2=selected_quotes["median_q"]["a"],
        quote_text_3=selected_quotes["long_q"]["q"],
        quote_author_3=selected_quotes["long_q"]["a"],
        date=date,
    )
    date_newsletter_path = object_storage_path / f"{date}_newsletter.txt"
    date_newsletter_path.write_text(newsletter)

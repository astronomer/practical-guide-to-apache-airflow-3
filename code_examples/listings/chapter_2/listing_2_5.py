import os  # A

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
    uri=(
        f"{OBJECT_STORAGE_SYSTEM}://",
        f"{OBJECT_STORAGE_PATH_NEWSLETTER}/",
        "DATE_newsletter.txt",
    ),  # C
    schedule=selected_quotes,
)
def formatted_newsletter(
    selected_quotes, **context
):  # D
    """
    Formats the newsletter.
    """
    from airflow.io.path import ObjectStoragePath  # E

    object_storage_path = ObjectStoragePath(
        f"{OBJECT_STORAGE_SYSTEM}://{OBJECT_STORAGE_PATH_NEWSLETTER}",
        conn_id=OBJECT_STORAGE_CONN_ID,
    )  # F

    date = context["dag_run"].run_after.strftime(
        "%Y-%m-%d"
    )  # G

    newsletter_template_path = (
        object_storage_path / "newsletter_template.txt"
    )

    newsletter_template = (
        newsletter_template_path.read_text()
    )  # H

    newsletter = newsletter_template.format(
        quote_text_1=selected_quotes["short_q"]["q"],
        quote_author_1=selected_quotes["short_q"]["a"],
        quote_text_2=selected_quotes["median_q"]["q"],
        quote_author_2=selected_quotes["median_q"]["a"],
        quote_text_3=selected_quotes["long_q"]["q"],
        quote_author_3=selected_quotes["long_q"]["a"],
        date=date,
    )  # I

    date_newsletter_path = (
        object_storage_path / f"{date}_newsletter.txt"
    )

    date_newsletter_path.write_text(newsletter)  # J


# A Put additional import statements at the top of the file
# B Define environment variables at the top of the file
# C Optional URI to identify the location of the data object referenced to by the asset
# D Passing the output of the upstream asset as well as the Airflow context into the function
# E Airflow object storage is used to interact with file storage (see below)
# F Define the object storage solution and path to interact with
# G Fetch the date of the dag run from the context, relevant for idempotency (see below)
# H Read the text from the newsletter template
# I Format the template text with today’s quotes
# J Write the full newsletter to a new file

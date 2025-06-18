import logging
import requests
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowSkipException

CSV_SUFFIX = ".csv"
TIMEOUT = 10  # seconds

@task
def st_extract():
    """
    Task to extract CSV data from a dynamically constructed URL based on execution date.

    Fetches CSV data from a URL formed by BASE_URL Airflow Variable + execution date (ds_nodash) + '.csv' suffix.
    Handles HTTP 404 by skipping the task and raises on other HTTP errors.
    Skips task if CSV content is empty.

    Returns:
        str: CSV data content decoded as UTF-8.

    Raises:
        AirflowSkipException: If data is not available (404) or CSV is empty.
        requests.exceptions.RequestException: For other HTTP errors or request failures.
    """
    context = get_current_context()
    ds_nodash = context["ds_nodash"]

    BASE_URL = Variable.get("BASE_URL")
    url = f"{BASE_URL}{ds_nodash}{CSV_SUFFIX}"

    logging.info(f"Fetching data from URL: {url}")

    try:
        response = requests.get(url, timeout=TIMEOUT)

        if response.status_code == 404:
            logging.warning(f"No data available for {ds_nodash} (404 Not Found). Skipping.")
            raise AirflowSkipException(f"No data available for {ds_nodash}.")

        response.raise_for_status()

        csv_data = response.content.decode("utf-8")

        if not csv_data.strip():
            logging.warning(f"CSV file for {ds_nodash} is empty. Skipping.")
            raise AirflowSkipException(f"Empty CSV file for {ds_nodash}.")

        logging.info(f"Data successfully retrieved for date {ds_nodash}")
        return csv_data

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error while fetching data: {e}", exc_info=True)
        raise

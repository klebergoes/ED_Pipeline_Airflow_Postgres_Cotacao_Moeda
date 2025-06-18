import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import io
import logging

@task
def st_load(data: str, ds: str):
    """
    Task to load transformed staging data into a PostgreSQL table.

    This task:
    - Validates the input
    - Deletes existing data for the execution date (ds)
    - Inserts new rows from the transformed DataFrame

    Args:
        data (str): JSON string with records to load.
        ds (str): Execution date (used to partition/overwrite data in staging).

    Raises:
        ValueError: If the execution date is missing.
        Exception: If any error occurs during the loading process.
    """
    if not ds:
        raise ValueError("Execution date (ds) must be provided.")

    try:
        if not data or not data.strip():
            logging.warning("No JSON data received from 'st_transform'. Skipping 'st_load'.")
            return None

        data_df = pd.read_json(io.StringIO(data), orient="records")

        if data_df.empty:
            logging.warning("DataFrame is empty. No records will be loaded.")
            return None

        # Ensure datetime columns are in correct format
        data_df["data_fechamento"] = pd.to_datetime(data_df["data_fechamento"])
        data_df["data_processamento"] = pd.to_datetime(data_df["data_processamento"])

        # Set up Postgres connection
        hook = PostgresHook(postgres_conn_id="postgres_astro", schema="staging")

        # Delete old records for this date (partition overwrite logic)
        delete_sql = "DELETE FROM staging.cotacoes WHERE data_fechamento = %s;"
        hook.run(delete_sql, parameters=(ds,))
        logging.info(f"Deleted existing records from staging.cotacoes for date {ds}.")

        # Prepare for bulk insert
        table_name = "staging.cotacoes"
        rows = list(data_df.itertuples(index=False, name=None))

        hook.insert_rows(
            table=table_name,
            rows=rows,
            target_fields=data_df.columns.tolist()
        )

        logging.info(f"{len(rows)} records inserted into {table_name}.")

    except Exception:
        logging.exception("Error while loading data into staging.")
        raise

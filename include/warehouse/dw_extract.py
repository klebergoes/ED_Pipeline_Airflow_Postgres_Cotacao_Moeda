from airflow.decorators import task
from include.utils.db import get_postgres_hook
import pandas as pd
import logging

@task
def dw_extract(ds: str):
    """
    Task to extract data from the 'staging.cotacoes' table for a specific date.

    Args:
        ds (str): Execution date in 'YYYY-MM-DD' format.

    Returns:
        str: JSON string representing records extracted from the database.
             Returns an empty JSON array "[]" if no records are found.

    Raises:
        Exception: If any error occurs during data extraction.
    """
    hook = get_postgres_hook(schema="staging")

    query = f"""
    SELECT
        data_fechamento,
        moeda_codigo,
        moeda_tipo,
        moeda_descricao,
        taxa_compra,
        taxa_venda,
        paridade_compra,
        paridade_venda,
        data_processamento
    FROM staging.cotacoes
    WHERE data_fechamento = DATE '{ds}';
    """

    try:
        logging.info(f"Extracting data from staging for date {ds}...")
        engine = hook.get_sqlalchemy_engine()
        df = pd.read_sql(query, con=engine)

        if df.empty:
            logging.warning(f"No data found for date {ds}. Returning empty JSON array.")
            return "[]"

        logging.info(f"Extracted {len(df)} records.")
        return df.to_json(orient="records")

    except Exception as e:
        logging.error(f"Error during data extraction: {e}", exc_info=True)
        raise

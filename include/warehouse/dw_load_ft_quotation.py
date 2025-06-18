from airflow.decorators import task
from airflow.operators.python import get_current_context
from include.utils.db import get_postgres_hook
import pandas as pd
from io import StringIO
import logging

@task
def dw_load_ft_quotation(data: str):
    """
    Load or update fact quotations data into the 'dw.ft_cotacao' table using UPSERT.

    Args:
        data (str): JSON string containing quotation records to load.

    Process:
        - Parses JSON into a pandas DataFrame.
        - Converts date columns to datetime objects.
        - Performs batch UPSERT into the database table based on (data_fechamento, moeda_codigo) keys.
        - Commits transaction after successful insertion/update.

    Raises:
        Exception: Raises any exception encountered during database operations.
    """

    if not data or not data.strip():
        logging.warning("No transformed data received to load in dw_load_ft_quotation. Skipping task.")
        return

    try:
        # Load data into DataFrame
        df = pd.read_json(StringIO(data), orient="records")

        # Convert timestamps - adjust 'unit' if your dates are ISO strings instead of epoch ms
        df['data_fechamento'] = pd.to_datetime(df['data_fechamento'], unit='ms', errors='coerce').dt.to_pydatetime()

        if 'data_processamento' in df.columns:
            df['data_processamento'] = pd.to_datetime(df['data_processamento'], unit='ms', errors='coerce').dt.to_pydatetime()
        else:
            df['data_processamento'] = pd.Timestamp.now().to_pydatetime()

        # Prepare data for insertion
        values = df.values.tolist()

        sql = """
            INSERT INTO dw.ft_cotacao (
                data_fechamento, moeda_codigo, taxa_compra, taxa_venda,
                paridade_compra, paridade_venda, data_processamento
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (data_fechamento, moeda_codigo)
            DO UPDATE SET
                taxa_compra = EXCLUDED.taxa_compra,
                taxa_venda = EXCLUDED.taxa_venda,
                paridade_compra = EXCLUDED.paridade_compra,
                paridade_venda = EXCLUDED.paridade_venda,
                data_processamento = EXCLUDED.data_processamento;
        """

        # Database connection and execution
        hook = get_postgres_hook(schema="dw")
        conn = hook.get_conn()
        with conn.cursor() as cursor:
            logging.info(f"Starting UPSERT of {len(values)} records into table dw.ft_cotacao...")
            cursor.executemany(sql, values)
        conn.commit()
        logging.info("UPSERT operation on dw.ft_cotacao completed successfully.")

    except Exception as e:
        logging.error(f"Error during UPSERT on fact table dw.ft_cotacao: {e}")
        raise

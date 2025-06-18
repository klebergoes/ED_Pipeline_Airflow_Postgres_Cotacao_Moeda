import pandas as pd
from airflow.decorators import task
from airflow.operators.python import get_current_context
import io
import pendulum
import logging

@task
def st_transform(data: str):
    """
    Task to transform raw CSV data into a cleaned JSON format.

    Args:
        data (str): Raw CSV data as a string.

    Returns:
        str | None: JSON string with transformed records, or None if input data is empty.

    Raises:
        Exception: If any error occurs during transformation.
    """
    if not data or not data.strip():
        logging.warning("No data returned from 'st_extract'. Skipping 'st_transform'.")
        return None

    try:
        csv_string_io = io.StringIO(data)

        column_names = [
            "data_fechamento", 
            "moeda_codigo", 
            "moeda_tipo", 
            "moeda_descricao",
            "taxa_compra", 
            "taxa_venda", 
            "paridade_compra", 
            "paridade_venda"
        ]

        data_types = {
            "moeda_codigo": int,
            "moeda_tipo": str, 
            "moeda_descricao": str,
            "taxa_compra": float, 
            "taxa_venda": float,
            "paridade_compra": float, 
            "paridade_venda": float
        }

        df = pd.read_csv(
            csv_string_io,
            sep=";", 
            decimal=",", 
            thousands=".", 
            header=None, 
            names=column_names,
            dtype=data_types,
            encoding="utf-8"
        )

        df["data_fechamento"] = pd.to_datetime(df["data_fechamento"], dayfirst=True)
        df["data_processamento"] = pendulum.now("America/Sao_Paulo").to_datetime_string()

        logging.info(f"{len(df)} records successfully transformed.")
        return df.to_json(orient="records", date_format="iso")

    except Exception:
        logging.exception("Error during data transformation.")
        raise

from airflow.decorators import task
import pandas as pd
import io
import logging

@task
def dw_transform_ft_quotation(data: str, ds: str):
    """
    Task to transform extracted data into a format suitable for the fact table 'dw.ft_cotacao'.

    Args:
        data (str): JSON string with extracted records.
        ds (str): Execution date (YYYY-MM-DD).

    Returns:
        str: JSON string of filtered and transformed records ready for fact table loading.

    Raises:
        ValueError: If input data is empty or invalid.
        Exception: For unexpected transformation errors.
    """
    if not data or not data.strip():
        raise ValueError("No data received from 'dw_extract'.")

    try:
        df = pd.read_json(io.StringIO(data), orient="records")

        logging.info(f"Received execution date (ds): {ds}")
        logging.info(f"'data_fechamento' dtype: {df['data_fechamento'].dtype}")
        logging.info(f"Unique dates before filtering: {df['data_fechamento'].unique()}")

        # Convert datetime format depending on format (string or timestamp)
        if pd.api.types.is_numeric_dtype(df["data_fechamento"]):
            df["data_fechamento"] = pd.to_datetime(df["data_fechamento"], unit="ms")
        else:
            df["data_fechamento"] = pd.to_datetime(df["data_fechamento"])

        # Normalize dates to remove time component
        df["data_fechamento"] = df["data_fechamento"].dt.normalize()
        ds_date = pd.to_datetime(ds).normalize()

        # Filter by execution date
        df = df[df["data_fechamento"] == ds_date]
        logging.info(f"Records after date filter ({ds}): {len(df)}")

        if df.empty:
            logging.warning(f"No quotation data found for date {ds}.")
            return "[]"

        # Ensure moeda_codigo is integer
        df["moeda_codigo"] = pd.to_numeric(df["moeda_codigo"], errors="coerce")
        df = df.dropna(subset=["moeda_codigo"])
        df["moeda_codigo"] = df["moeda_codigo"].astype(int)

        # Select only fact table columns
        df_fact = df[[
            "data_fechamento",
            "moeda_codigo",
            "taxa_compra",
            "taxa_venda",
            "paridade_compra",
            "paridade_venda",
            "data_processamento"
        ]]

        logging.info(f"{len(df_fact)} records prepared for fact table loading.")
        return df_fact.to_json(orient="records")

    except Exception:
        logging.exception("Error during fact table transformation.")
        raise

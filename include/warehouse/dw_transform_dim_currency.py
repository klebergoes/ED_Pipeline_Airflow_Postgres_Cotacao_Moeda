from airflow.decorators import task
from airflow.operators.python import get_current_context
import pandas as pd
from io import StringIO
import pendulum
import logging

@task
def dw_transform_dim_currency(data: str):
    """
    Task to transform raw currency data into a dimension format (dim_moeda),
    applying SCD2 (Slowly Changing Dimension Type 2) structure.

    Args:
        data (str): JSON string with currency records from the fact table.

    Returns:
        str | None: Transformed JSON string ready for SCD2 loading,
                    or None if input data is empty.
    """
    if not data or not data.strip():
        logging.warning("No data extracted. Task 'dw_transform_dim_currency' will be skipped.")
        return None

    try:
        # Convert JSON string to pandas DataFrame
        df = pd.read_json(StringIO(data), orient="records")

        # Clean and standardize string columns
        df["moeda_codigo"] = df["moeda_codigo"].astype(str).str.strip()
        df["moeda_tipo"] = df["moeda_tipo"].astype(str).str.strip()
        df["moeda_descricao"] = df["moeda_descricao"].astype(str).str.strip()

        # Simulate a change to test SCD2 logic (e.g. description change for currency 220)
        # df.loc[df["moeda_codigo"] == "220", "moeda_descricao"] = "USD-3"

        # Add SCD2 control columns
        df["data_inicio"] = pendulum.now("America/Sao_Paulo").to_datetime_string()
        df["data_fim"] = None
        df["registro_ativo"] = True

        # Select only dimension-relevant fields and remove duplicates
        df_dim = df[[
            "moeda_codigo",
            "moeda_tipo",
            "moeda_descricao",
            "data_inicio",
            "data_fim",
            "registro_ativo"
        ]].drop_duplicates().copy()

        logging.info(f"{len(df_dim)} distinct currency records prepared for SCD2 load.")
        return df_dim.to_json(orient="records")

    except Exception:
        logging.exception("Error while transforming the currency dimension.")
        raise

from airflow.decorators import task
from include.utils.db import get_postgres_hook
import logging

CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS dw;"
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dw.ft_cotacao (
    data_fechamento DATE,
    moeda_codigo INT,
    taxa_compra NUMERIC(15,6),
    taxa_venda NUMERIC(15,6),
    paridade_compra NUMERIC(15,6),
    paridade_venda NUMERIC(15,6),
    data_processamento TIMESTAMP DEFAULT NOW(),
    CONSTRAINT ft_cotacao_pk PRIMARY KEY (data_fechamento, moeda_codigo)
);
"""

@task
def dw_create_table_ft_quotation():
    """
    Task to create the 'dw' schema and the 'dw.ft_cotacao' fact table in the PostgreSQL database.

    Executes SQL commands to create the schema and table if they do not exist.

    Returns:
        str: Confirmation message indicating successful table creation.

    Raises:
        Exception: If there is an error during schema or table creation.
    """
    hook = get_postgres_hook(schema="dw")

    try:
        logging.info("Creating schema 'dw' if it does not exist...")
        hook.run(CREATE_SCHEMA_SQL, autocommit=True)

        logging.info("Creating fact table 'dw.ft_cotacao' if it does not exist...")
        hook.run(CREATE_TABLE_SQL, autocommit=True)

        msg = "Fact table 'dw.ft_cotacao' created successfully."
        logging.info(msg)
        return msg

    except Exception as e:
        logging.error(f"Error creating fact table 'dw.ft_cotacao': {e}", exc_info=True)
        raise

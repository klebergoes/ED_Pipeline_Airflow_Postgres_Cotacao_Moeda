from airflow.decorators import task
from include.utils.db import get_postgres_hook
import logging

CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS staging;"
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS staging.cotacoes (
    data_fechamento DATE,
    moeda_codigo INT,
    moeda_tipo TEXT,
    moeda_descricao TEXT,
    taxa_compra NUMERIC(15,6),
    taxa_venda NUMERIC(15,6),
    paridade_compra NUMERIC(15,6),
    paridade_venda NUMERIC(15,6),
    data_processamento TIMESTAMP
);
"""

@task
def st_create_table():
    """
    Task to create the 'staging' schema and 'staging.cotacoes' table in the PostgreSQL database.

    Uses a PostgresHook to run SQL commands that create schema and table if they do not exist.

    Returns:
        str: Success message confirming table creation.

    Raises:
        Exception: If there is any error during schema or table creation.
    """
    hook = get_postgres_hook(schema="staging")

    try:
        logging.info("Creating schema 'staging' if it does not exist...")
        hook.run(CREATE_SCHEMA_SQL, autocommit=True)

        logging.info("Creating table 'staging.cotacoes' if it does not exist...")
        hook.run(CREATE_TABLE_SQL, autocommit=True)

        msg = "Table 'staging.cotacoes' created successfully."
        logging.info(msg)
        return msg

    except Exception as e:
        logging.error(f"Error creating schema or table: {e}", exc_info=True)
        raise

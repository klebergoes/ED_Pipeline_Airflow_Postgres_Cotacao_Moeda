from airflow.decorators import task
from include.utils.db import get_postgres_hook
import logging

CREATE_SCHEMA_SQL = "CREATE SCHEMA IF NOT EXISTS dw;"
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS dw.dim_moeda (
    moeda_id INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    moeda_codigo INT,
    moeda_tipo TEXT,
    moeda_descricao TEXT,
    data_inicio DATE NOT NULL,
    data_fim DATE,
    registro_ativo BOOLEAN DEFAULT TRUE
);
"""

@task
def dw_create_table_dim_currency():
    """
    Task to create the 'dw' schema and 'dw.dim_moeda' dimension table in the PostgreSQL database.

    Uses PostgresHook to execute SQL commands that create schema and table if they don't exist.

    Returns:
        str: Success message confirming the table creation.

    Raises:
        Exception: If an error occurs during schema or table creation.
    """
    hook = get_postgres_hook(schema="dw")

    try:
        logging.info("Creating schema 'dw' if it does not exist...")
        hook.run(CREATE_SCHEMA_SQL, autocommit=True)

        logging.info("Creating table 'dw.dim_moeda' if it does not exist...")
        hook.run(CREATE_TABLE_SQL, autocommit=True)

        msg = "Table 'dw.dim_moeda' created successfully."
        logging.info(msg)
        return msg

    except Exception as e:
        logging.error(f"Error creating schema or table: {e}", exc_info=True)
        raise

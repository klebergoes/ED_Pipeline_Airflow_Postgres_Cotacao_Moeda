from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "postgres_astro"
POSTGRES_SCHEMA = "astro"

def get_postgres_hook(schema: str = POSTGRES_SCHEMA):
    """
    Returns a PostgresHook configured with the default connection ID and schema.

    Args:
        schema (str): The schema to be used in the connection (default: "astro").

    Returns:
        PostgresHook: A configured instance of the PostgreSQL connection hook.
    """
    try:
        return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID, schema=schema)
    except Exception as e:
        raise RuntimeError(f"Failed to connect to Postgres with conn_id='{POSTGRES_CONN_ID}' and schema='{schema}': {e}")

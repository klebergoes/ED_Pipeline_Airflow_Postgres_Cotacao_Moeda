from airflow.decorators import task
from airflow.operators.python import get_current_context
from include.utils.db import get_postgres_hook
import pandas as pd
from io import StringIO
import logging
import pendulum

@task
def dw_load_dim_currency(data: str):
    """
    Task to load dimension data into the 'dw.dim_moeda' table using Slowly Changing Dimension Type 2 logic.

    Steps:
    - For each record, deactivate any previous active row if the description or type has changed
    - Insert a new active row if the current values do not exist
    - Retrieve the current 'moeda_id' for each currency

    Args:
        data (str): JSON string with currency dimension records.

    Returns:
        list[dict]: List of dictionaries containing 'moeda_codigo' and resolved 'moeda_id' for each entry.
    """
    if not data or not data.strip():
        logging.warning("No transformed data received. Skipping 'dw_load_dim_currency' task.")
        return

    df = pd.read_json(StringIO(data), orient="records")
    hook = get_postgres_hook(schema="dw")
    engine = hook.get_sqlalchemy_engine()

    results = []
    today = pendulum.now("America/Sao_Paulo").date().isoformat()

    logging.info(f"Starting SCD2 load for {len(df)} currency dimension records.")

    with engine.begin() as conn:
        for _, row in df.iterrows():
            moeda_codigo = row["moeda_codigo"]
            moeda_tipo = row["moeda_tipo"]
            moeda_descricao = row["moeda_descricao"]

            # Step 1: Deactivate previous row if changed
            update_sql = """
                UPDATE dw.dim_moeda
                SET registro_ativo = FALSE,
                    data_fim = (%(data_inicio)s::date - INTERVAL '1 day')
                WHERE moeda_codigo = %(moeda_codigo)s
                  AND registro_ativo = TRUE
                  AND (moeda_tipo <> %(moeda_tipo)s OR moeda_descricao <> %(moeda_descricao)s);
            """
            conn.execute(update_sql, {
                "data_inicio": today,
                "moeda_codigo": moeda_codigo,
                "moeda_tipo": moeda_tipo,
                "moeda_descricao": moeda_descricao
            })

            # Step 2: Insert new row if not exists
            insert_sql = """
                INSERT INTO dw.dim_moeda (
                    moeda_codigo, moeda_tipo, moeda_descricao,
                    data_inicio, data_fim, registro_ativo
                )
                SELECT %(moeda_codigo)s, %(moeda_tipo)s, %(moeda_descricao)s,
                       %(data_inicio)s::date, NULL, TRUE
                WHERE NOT EXISTS (
                    SELECT 1 FROM dw.dim_moeda
                    WHERE moeda_codigo = %(moeda_codigo)s
                      AND registro_ativo = TRUE
                      AND moeda_tipo = %(moeda_tipo)s
                      AND moeda_descricao = %(moeda_descricao)s
                )
                RETURNING moeda_id;
            """
            result = conn.execute(insert_sql, {
                "moeda_codigo": moeda_codigo,
                "moeda_tipo": moeda_tipo,
                "moeda_descricao": moeda_descricao,
                "data_inicio": today
            })

            row_result = result.fetchone()
            if row_result:
                moeda_id = row_result[0]
            else:
                # Fallback: select existing active ID
                select_sql = """
                    SELECT moeda_id
                    FROM dw.dim_moeda
                    WHERE moeda_codigo = %(moeda_codigo)s
                      AND registro_ativo = TRUE
                      AND moeda_tipo = %(moeda_tipo)s
                      AND moeda_descricao = %(moeda_descricao)s
                    LIMIT 1;
                """
                row_select = conn.execute(select_sql, {
                    "moeda_codigo": moeda_codigo,
                    "moeda_tipo": moeda_tipo,
                    "moeda_descricao": moeda_descricao
                }).fetchone()
                moeda_id = row_select[0] if row_select else None

            if moeda_id:
                results.append({
                    "moeda_codigo": moeda_codigo,
                    "moeda_id": moeda_id
                })
                logging.info(f"SCD2 updated for moeda_codigo={moeda_codigo}, moeda_id={moeda_id}")
            else:
                logging.warning(f"Failed to resolve moeda_id for moeda_codigo={moeda_codigo}")

    logging.info("SCD2 load for currency dimension completed.")
    return results

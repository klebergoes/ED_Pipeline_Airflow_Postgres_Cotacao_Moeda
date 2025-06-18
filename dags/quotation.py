from airflow.decorators import dag, task_group
import pendulum

# Importação dos módulos de tasks por área (staging e data warehouse)
from include.staging.st_create_table import st_create_table
from include.staging.st_extract import st_extract
from include.staging.st_transform import st_transform
from include.staging.st_load import st_load

from include.warehouse.dw_create_table_dim_currency import dw_create_table_dim_currency
from include.warehouse.dw_create_table_ft_quotation import dw_create_table_ft_quotation
from include.warehouse.dw_extract import dw_extract
from include.warehouse.dw_transform_dim_currency import dw_transform_dim_currency
from include.warehouse.dw_transform_ft_quotation import dw_transform_ft_quotation
from include.warehouse.dw_load_dim_currency import dw_load_dim_currency
from include.warehouse.dw_load_ft_quotation import dw_load_ft_quotation


default_args = {
    "owner": "kleber",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


@task_group(group_id='Create_Tables')
def create_table_group():
    """
    Task group to create necessary schemas and tables in staging and DW.
    """
    st_create_table()
    dw_create_table_dim_currency()
    dw_create_table_ft_quotation()


@task_group(group_id='Staging_Area')
def staging_group():
    """
    Task group for staging area pipeline:
    - Extract raw data from source
    - Transform raw data into structured format
    - Load transformed data into staging tables
    """
    extract_staging = st_extract()
    transform_staging = st_transform(extract_staging)

    # Ensures transform_staging runs only if extract_staging succeeded or skipped
    transform_staging.trigger_rule = "none_failed_or_skipped"
    load_staging = st_load(transform_staging, ds="{{ ds }}")

    extract_staging >> transform_staging >> load_staging


@task_group(group_id='DW_Area')
def dw_group():
    """
    Task group for Data Warehouse pipeline:
    - Extract data from staging
    - Transform data for dimension and fact tables
    - Load transformed data into dimension and fact tables with SCD2 logic and UPSERT
    """
    extract_dw = dw_extract()

    transform_dw_dim_currency = dw_transform_dim_currency(extract_dw)
    transform_dw_ft_quotation = dw_transform_ft_quotation(extract_dw, ds="{{ ds }}")

    load_dw_dim_currency = dw_load_dim_currency(transform_dw_dim_currency)
    load_dw_ft_quotation = dw_load_ft_quotation(transform_dw_ft_quotation)

    extract_dw >> [transform_dw_dim_currency, transform_dw_ft_quotation]
    transform_dw_dim_currency >> load_dw_dim_currency
    transform_dw_ft_quotation >> load_dw_ft_quotation


@dag(
    dag_id="fin_quotation_bcb_classic",
    description="Classic pipeline for ingesting exchange rate data from BCB",
    default_args=default_args,
    schedule="0 14 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["bcb", "finance"],
)
def pipeline():
    """
    Main DAG function to orchestrate the financial quotation pipeline:
    - Create schemas and tables
    - Run staging extraction, transformation, loading
    - Run data warehouse extraction, transformation, loading
    """
    create_tables = create_table_group()
    staging_area = staging_group()
    dw_area = dw_group()

    create_tables >> staging_area >> dw_area

dag = pipeline()
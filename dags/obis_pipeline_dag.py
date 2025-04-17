from __future__ import annotations

import datetime
import pendulum
import json
import os
import logging

# Airflow specific imports
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from airflow.exceptions import AirflowFailException
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLCheckOperator
from airflow.utils.email import send_email

try:
    from scripts.obis_etl_functions import (
        fetch_obis_deepsea_data,
        transform_and_split_data,
        prepare_postgres_schema,
        load_postgres_normalized_data
    )
    FUNCTIONS_LOADED = True
except ImportError as e:
    logging.error(f"Failed to import ETL functions: {e}. Check scripts folder structure and __init__.py.")
    FUNCTIONS_LOADED = False
    def fetch_obis_deepsea_data(*args, **kwargs): raise NotImplementedError("Import failed")
    def transform_and_split_data(*args, **kwargs): raise NotImplementedError("Import failed")
    def prepare_postgres_schema(*args, **kwargs): raise NotImplementedError("Import failed")
    def load_postgres_normalized_data(*args, **kwargs): raise NotImplementedError("Import failed")

# --- Define Config ---
# Paths accessible within Airflow workers (using volume mapped to ./store)
DATA_STORE_PATH = "/opt/airflow/store" 
RAW_DATA_PATH = os.path.join(DATA_STORE_PATH, "obis_raw_data.json")
TRANSFORMED_FILES = {
    "species": os.path.join(DATA_STORE_PATH, "species.parquet"),
    "datasets": os.path.join(DATA_STORE_PATH, "datasets.parquet"),
    "records": os.path.join(DATA_STORE_PATH, "record_details.parquet"),
    "occurrences": os.path.join(DATA_STORE_PATH, "occurrences.parquet"),
}

# Read ETL Parameters from Airflow Variables
try:
    OBIS_MIN_DEPTH = int(Variable.get("obis_min_depth", default_var=1000))
    OBIS_MAX_RECORDS = int(Variable.get("obis_max_records", default_var=5000)) 
except KeyError:
    logging.warning("Airflow Variables 'obis_min_depth' or 'obis_max_records' not set. Using defaults.")
    OBIS_MIN_DEPTH = 1000
    OBIS_MAX_RECORDS = 5000

# Airflow Connection IDs (must be configured in Airflow UI)
POSTGRES_CONN_ID = "obis_postgres"
pg_db_name = "airflow"
pg_schema = "public"

ds_fact_occurrences = Dataset(f"postgresql://{POSTGRES_CONN_ID}/{pg_db_name}/{pg_schema}/fact_occurrences")
ds_dim_species = Dataset(f"postgresql://{POSTGRES_CONN_ID}/{pg_db_name}/{pg_schema}/dim_species")
ds_dim_datasets = Dataset(f"postgresql://{POSTGRES_CONN_ID}/{pg_db_name}/{pg_schema}/dim_datasets")
ds_dim_record_details = Dataset(f"postgresql://{POSTGRES_CONN_ID}/{pg_db_name}/{pg_schema}/dim_record_details")

def _extract_data():
    """Fetches data and saves to JSON file in the shared store."""
    if not FUNCTIONS_LOADED: raise ImportError("ETL functions failed to load.")
    logging.info(f"Starting extraction: min_depth={OBIS_MIN_DEPTH}, max_records={OBIS_MAX_RECORDS}")
    fetched_data = fetch_obis_deepsea_data(
        min_depth=OBIS_MIN_DEPTH, 
        max_records_to_fetch=OBIS_MAX_RECORDS
    )
    if fetched_data:
        logging.info(f"Fetched {len(fetched_data)} records. Saving raw data to {RAW_DATA_PATH}")
        os.makedirs(os.path.dirname(RAW_DATA_PATH), exist_ok=True)
        with open(RAW_DATA_PATH, 'w') as f:
            json.dump(fetched_data, f) 
        logging.info("Raw data save complete.")
    else:
        logging.warning("No data fetched from OBIS.")
        raise AirflowFailException("Extraction failed or returned no data.")

def _transform_and_split():
    """Loads raw data, transforms & splits it, saves multiple Parquet files."""
    if not FUNCTIONS_LOADED: raise ImportError("ETL functions failed to load.")
    logging.info(f"Starting transformation & splitting from {RAW_DATA_PATH}")
    
    if not os.path.exists(RAW_DATA_PATH):
         raise FileNotFoundError(f"Raw data file not found for transformation: {RAW_DATA_PATH}")
         
    dataframes_dict = transform_and_split_data(RAW_DATA_PATH) 
    
    if dataframes_dict is None:
        raise AirflowFailException("Transformation/splitting function failed or returned None.")
    if not isinstance(dataframes_dict, dict) or not dataframes_dict:
        raise AirflowFailException("Transformation/splitting did not return a valid dictionary of DataFrames.")

    logging.info(f"Transformation successful. Saving DataFrames: {list(dataframes_dict.keys())}")
    os.makedirs(DATA_STORE_PATH, exist_ok=True) 

    saved_files = 0
    for name, df in dataframes_dict.items():
        output_path = TRANSFORMED_FILES.get(name)
        if output_path is None:
            logging.warning(f"No output path defined for transformed DataFrame '{name}', skipping save.")
            continue
            
        if df is not None and df.height > 0:
            logging.info(f"Saving {name} data ({df.shape}) to {output_path}...")
            try:
                df.write_parquet(output_path)
                saved_files += 1
                logging.info(f"Saved {output_path}.")
            except Exception as e:
                 logging.error(f"Failed to save DataFrame '{name}' to {output_path}: {e}")
                 raise 
        else:
            logging.warning(f"DataFrame for '{name}' is empty or None, skipping save. Creating empty file placeholder.")
            try:
                 import polars as pl
                 pl.DataFrame().write_parquet(output_path)
            except Exception as e_write:
                 logging.error(f"Could not write empty placeholder for {output_path}: {e_write}")
            
    if saved_files == 0:
         raise AirflowFailException("No valid DataFrames were saved after transformation.")
    logging.info("Finished saving transformed parquet files.")
    
def _prepare_postgres_schema():
    """Ensures all target PostgreSQL tables exist using Airflow Connection."""
    if not FUNCTIONS_LOADED: raise ImportError("ETL functions failed to load.")
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn_uri = pg_hook.get_uri()
        logging.info("Ensuring all PostgreSQL tables exist via SQL definitions...")
        success = prepare_postgres_schema(conn_uri) 
        if not success: 
            raise RuntimeError("prepare_postgres_schema indicated failure.")
    except Exception as e:
         logging.error(f"Error in _prepare_postgres_schema task: {e}")
         raise AirflowFailException(f"Failed to prepare PostgreSQL schema: {e}")
     
def _load_postgres_normalized():
    """Loads multiple Parquet files into normalized PostgreSQL tables."""
    if not FUNCTIONS_LOADED: raise ImportError("ETL functions failed to load.")
    try:
        logging.info("Loading normalized data into PostgreSQL...")
        load_postgres_normalized_data(POSTGRES_CONN_ID, DATA_STORE_PATH)
        logging.info("Normalized PostgreSQL loading finished.")
    except Exception as e:
         logging.error(f"Error in _load_postgres_normalized task: {e}")
         raise AirflowFailException(f"Failed to load normalized data to PostgreSQL: {e}")

def _check_postgres_load():
    """Checks row counts in the normalized PostgreSQL tables."""
    logging.info("Performing basic data quality checks on normalized tables...")
    check_passed = True
    try:
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        fact_count = pg_hook.get_first("SELECT COUNT(*) FROM fact_occurrences;")[0]
        species_count = pg_hook.get_first("SELECT COUNT(*) FROM dim_species;")[0]
        logging.info(f"Postgres Check Counts: fact_occurrences={fact_count}, dim_species={species_count}")
        
        if fact_count == 0 or species_count == 0: 
            check_passed = False
            logging.error("Load check failed: Zero rows found in critical normalized tables.")
        else:
            logging.info("PostgreSQL Load Check PASSED.")
            
    except Exception as e:
        logging.error(f"Error during PostgreSQL check: {e}")
        check_passed = False 
        
    if not check_passed:
         raise AirflowFailException("Data quality check failed for PostgreSQL.")

# Let's drafted a failure email message 
# We will send to email that we want, you can modified RECEPIENT
RECEPIENT = "your_email@gmail.com"
SMTP_CONN_ID = "smtp_conn"

# Custom callback to send email on task failure using SMTP.
def failure_email(context):
    try:
        task_instance = context['task_instance']
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        execution_date = context['execution_date']
        log_url = task_instance.log_url
        
        task_status = 'Failed'
        subject = f"Airflow Alert: Task Failed - DAG: {dag_id}, Task: {task_id}"
        
        body = f"""
        <h3>Airflow Task Failure Alert</h3>
        <b>DAG:</b> {dag_id}<br>
        <b>Task:</b> {task_id}<br>
        <b>Status:</b> {task_status}<br>
        <b>Execution Date:</b> {execution_date}<br>
        <b>Log URL:</b> <a href="{log_url}" target="_blank">View Logs</a><br>
        <br>
        Task instance failed. Please check the logs for details.
        """
        
        to_email = [email.strip() for email in RECEPIENT.split(',') if email.strip()] 
        
        if not to_email:
             logging.warning("No recipient email configured for failure alert.")
             return

        logging.info(f"Sending failure email to {to_email} via connection '{SMTP_CONN_ID}'...")
        
        send_email(
            to=to_email, 
            subject=subject, 
            html_content=body,
            conn_id=SMTP_CONN_ID
        )
        
        logging.info(f"Failure email sent successfully via connection '{SMTP_CONN_ID}'.")

    except Exception as e:
        logging.error(f"Failed to send failure email via connection '{SMTP_CONN_ID}': {e}", exc_info=True)
        
# --- DAG Definition ---
with DAG(
    dag_id="obis_deep_sea_normalized_etl", 
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"), 
    schedule=None, # Manual trigger, for aumation check README.md
    catchup=False,
    tags=["etl", "obis", "deep-sea", "normalized", "portfolio"],
    default_args={
        "owner": "airflow", # Change as you needed
        "retries": 1,
        "retry_delay": datetime.timedelta(minutes=3),
        'email_on_failure': True,
        'email_on_retry': False,
    },
    description="ETL pipeline for OBIS data with normalized PostgreSQL schema.",
) as dag:

    # --- Define Tasks ---
    extract_task = PythonOperator(
        task_id="extract_obis_data",
        python_callable=_extract_data,
        on_failure_callback = failure_email,
    )

    transform_task = PythonOperator(
        task_id="transform_and_split_data", 
        python_callable=_transform_and_split,
        on_failure_callback = failure_email,
    )

    prepare_pg_schema_task = PythonOperator( 
        task_id="prepare_postgres_schema",
        python_callable=_prepare_postgres_schema,
        on_failure_callback = failure_email,
    )
    
    load_pg_normalized_task = PythonOperator( 
        task_id="load_to_postgres_normalized",
        python_callable=_load_postgres_normalized,
        on_failure_callback = failure_email,
        outlets=[
            ds_fact_occurrences, 
            ds_dim_species, 
            ds_dim_datasets, 
            ds_dim_record_details
        ] 
    )
    
    check_pg_task = PythonOperator(
    task_id="check_postgres_row_counts",
    python_callable=_check_postgres_load, 
    on_failure_callback = failure_email,
    )
    
    # Ensure essential Foreign Keys in fact table are not NULL
    check_fk_nulls = SQLCheckOperator(
        task_id="check_foreign_key_nulls",
        conn_id=POSTGRES_CONN_ID, 
        sql="""
        SELECT COUNT(*)
        FROM fact_occurrences
        WHERE obis_id IS NULL OR aphia_id IS NULL OR dataset_id IS NULL;
        """,
        on_failure_callback = failure_email,
    )
    
    # Ensure all depths are within the expected range (>= 1000)
    check_depth_range = SQLCheckOperator(
        task_id="check_depth_range",
        conn_id=POSTGRES_CONN_ID,
        sql="""
        SELECT COUNT(*) = 0
        FROM fact_occurrences 
        WHERE depth_m < 500;
        """,
        on_failure_callback = failure_email,
    )

    # --- Define Flow ---
    extract_task >> transform_task >> prepare_pg_schema_task >> load_pg_normalized_task
    load_pg_normalized_task >> [check_fk_nulls, check_depth_range, check_pg_task]
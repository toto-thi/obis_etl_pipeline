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
         
    # Call the function that returns the dictionary of DataFrames
    dataframes_dict = transform_and_split_data(RAW_DATA_PATH) 
    
    if dataframes_dict is None:
        raise AirflowFailException("Transformation/splitting function failed or returned None.")
    if not isinstance(dataframes_dict, dict) or not dataframes_dict:
        raise AirflowFailException("Transformation/splitting did not return a valid dictionary of DataFrames.")

    logging.info(f"Transformation successful. Saving DataFrames: {list(dataframes_dict.keys())}")
    os.makedirs(DATA_STORE_PATH, exist_ok=True) 

    saved_files = 0
    for name, df in dataframes_dict.items():
        output_path = TRANSFORMED_FILES.get(name) # Get path from our dict
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
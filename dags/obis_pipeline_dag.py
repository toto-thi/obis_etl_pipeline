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


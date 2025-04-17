import polars as pl
import logging
import os
from sqlalchemy import text
from sqlalchemy.engine import Engine

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def create_table(engine: Engine, create_sql: str, table_name_for_log: str):
    """Helper to execute a single CREATE TABLE IF NOT EXISTS statement."""
    try:
        with engine.connect() as connection:
            with connection.begin():
                 logging.debug(f"Executing CREATE TABLE IF NOT EXISTS for {table_name_for_log}...")
                 connection.execute(text(create_sql))
            logging.debug(f"Table '{table_name_for_log}' ensured.")
            return True
    except Exception as e:
        logging.error(f"Failed to create/ensure table '{table_name_for_log}': {e}")
        raise 

def load_table_from_parquet(
        parquet_path: str,
        table_name: str,
        conn_str: str, 
        if_table_exists_strategy: str = "append"
      ):
    """Loads data from a Parquet file into a specified PostgreSQL table using Polars."""
    if not os.path.exists(parquet_path):
        logging.warning(f"Parquet file not found: {parquet_path}. Skipping load for table {table_name}.")
        return True 

    df_to_load = None 
    try:
        df_to_load = pl.read_parquet(parquet_path)
        if df_to_load.height == 0:
            logging.info(f"Parquet file {parquet_path} is empty. No data to load for {table_name}.")
            return True 

        logging.info(f"Loading {df_to_load.height} rows from {parquet_path} into PostgreSQL table '{table_name}' (strategy: '{if_table_exists_strategy}')...")
        
        df_to_load.write_database(
            table_name=table_name,
            connection=conn_str,
            if_table_exists=if_table_exists_strategy,
        )
        logging.info(f"Successfully loaded data into '{table_name}'.")
        return True 

    except ImportError as e:
         logging.error(f"ImportError during write_database for {table_name} (connectorx/pyarrow?): {e}.")
         logging.error("Ensure 'connectorx', 'polars[pyarrow]', 'psycopg2-binary' are installed.")
         raise RuntimeError(f"Dependency missing for DB write for {table_name}") from e
    except Exception as e:
        logging.error(f"Error loading data into {table_name} from {parquet_path}: {e}")
        raise RuntimeError(f"Failed to load data into {table_name}") from e
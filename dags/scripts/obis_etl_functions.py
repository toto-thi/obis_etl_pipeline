import os
import requests
import logging
import time
import datetime
import polars as pl
from sqlalchemy import create_engine
from airflow.providers.postgres.hooks.postgres import PostgresHook 

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OBIS_API_URL = "https://api.obis.org/v3/occurrence"
MIN_DEPTH_METERS = 1000
RECORDS_PER_PAGE = 1000 
API_TIMEOUT_SECONDS = 120
POLITE_DELAY_SECONDS = 1


def fetch_obis_deepsea_data(min_depth: int, max_records_to_fetch: int):
    """
    Fetches deep sea occurrence data from OBIS API using cursor-based
    pagination ('size' and 'after').

    Args:
        min_depth: Minimum depth in meters.
        max_records_to_fetch: The maximum number of records to fetch in total.

    Returns:
        A list of dictionaries (occurrence records), or None/partial list on error.
    """
    all_records = []
    total_fetched = 0
    last_id_fetched = None # This will store the 'id' of the last record from the previous page
    fetch_attempts = 0 

    logging.info(f"Starting OBIS fetch: min_depth={min_depth}, max_records={max_records_to_fetch}")
    while total_fetched < max_records_to_fetch:
        fetch_attempts += 1
        if fetch_attempts > (max_records_to_fetch // RECORDS_PER_PAGE) + 20:
             logging.warning(f"Exceeded fetch attempts. Stopping.")
             break
        if last_id_fetched: time.sleep(POLITE_DELAY_SECONDS)
        current_size = min(RECORDS_PER_PAGE, max_records_to_fetch - total_fetched)
        if current_size <= 0: break
        params = {'startdepth': min_depth, 'size': current_size}
        if last_id_fetched: params['after'] = last_id_fetched
        
        logging.info(f"Requesting page: size={current_size}" + (f", after={last_id_fetched}" if last_id_fetched else ""))
        
        try:
            response = requests.get(OBIS_API_URL, params=params, headers={'Accept': 'application/json'}, timeout=API_TIMEOUT_SECONDS)
            response.raise_for_status()
            results = response.json()
            if not isinstance(results, list): results = results.get('results', []) # Handle potential wrapping
            if not isinstance(results, list) or not results: break # Exit if not list or empty
            all_records.extend(results)
            num_received = len(results)
            total_fetched += num_received
            if 'id' in results[-1] and results[-1]['id'] is not None:
                last_id_fetched = results[-1]['id']
            else: raise KeyError("Missing 'id' field")
            logging.info(f"Fetched {num_received}. Total: {total_fetched}/{max_records_to_fetch}. Next: {last_id_fetched}")
            if total_fetched >= max_records_to_fetch: break
        except Exception as e:
            logging.error(f"Error during fetch (last id {last_id_fetched}): {e}")
            break
    logging.info(f"Finished fetching. Collected: {len(all_records)}")
    return all_records if all_records else None

try:
    from .obis_normalization import (
        extract_species_dimension,
        extract_datasets_dimension,
        extract_record_details_dimension,
        create_occurrence_facts
    )
    NORMALIZATION_FUNCS_LOADED = True
except ImportError as e:
     logging.error(f"Could not import normalization functions: {e}")
     NORMALIZATION_FUNCS_LOADED = False
     
# Define columns to keep and their new names
COLUMNS_TO_KEEP_AND_RENAME = {
    "occurrence_id": "occurrenceID", # Primary key candidate
    "obis_id": "id",                 # OBIS internal record ID
    "scientific_name": "scientificName",
    "aphia_id": "aphiaID",           # World Register of Marine Species ID
    "kingdom": "kingdom",
    "phylum": "phylum",
    "class_name": "class",          # Renamed 'class' -> 'class_name'
    "order_name": "order",          # Renamed 'order' -> 'order_name'
    "family": "family",
    "latitude": "decimalLatitude",
    "longitude": "decimalLongitude",
    "depth_m": "depth",              # Assuming meters
    "event_date": "eventDate",       # Original YYYY-MM-DD string
    "event_ts_start": "date_start",  # Epoch ms (keep original name short)
    "event_ts_mid": "date_mid",      # Epoch ms
    "event_ts_end": "date_end",      # Epoch ms
    "basis_of_record": "basisOfRecord",
    "individual_count": "individualCount",
    "occurrence_status": "occurrenceStatus",
    "dataset_id": "datasetID",
    "references_url": "references",
}

def _load_and_deduplicate_raw_data(raw_data_path: str) -> pl.DataFrame | None:
    """Loads raw JSON data and deduplicates based on occurrenceID."""
    logging.info(f"Helper: Loading and deduplicating raw data from {raw_data_path}")
    if not os.path.exists(raw_data_path):
        logging.error(f"Raw data file not found: {raw_data_path}")
        return None
    try:
        df_raw = pl.read_json(raw_data_path, infer_schema_length=None)
        initial_row_count = df_raw.height
        logging.info(f"Loaded raw data. Shape: {df_raw.shape}")
        if initial_row_count == 0: return df_raw 

        # --- Duplicate ID Check ---
        id_column_raw = "occurrenceID" 
        if id_column_raw not in df_raw.columns:
             logging.error(f"Cannot deduplicate: Column '{id_column_raw}' not found.")
             raise ValueError(f"Missing critical ID column: {id_column_raw}") 
             
        unique_ids = df_raw[id_column_raw].n_unique()
        null_id_count = df_raw[id_column_raw].null_count()
        non_null_rows = initial_row_count - null_id_count
        duplicates_found = non_null_rows - unique_ids if non_null_rows >= unique_ids else 0
        if duplicates_found > 0: logging.warning(f"Found {duplicates_found} duplicate non-null '{id_column_raw}' values.")

        # --- Deduplication ---
        logging.info(f"Deduplicating raw data based on '{id_column_raw}'...")
        df_deduplicated = df_raw.unique(subset=[id_column_raw], keep="first", maintain_order=True)
        logging.info(f"Shape after deduplication: {df_deduplicated.shape}")
        return df_deduplicated
        
    except Exception as e:
        logging.error(f"Error in _load_and_deduplicate_raw: {e}", exc_info=True)
        return None 
    
def _select_and_rename(df: pl.DataFrame) -> pl.DataFrame:
    """Selects relevant columns and renames them."""
    logging.info("Helper: Selecting and renaming columns...")
    # ... (Selection/rename logic using df, COLUMNS_TO_KEEP_AND_RENAME - same as before) ...
    columns_to_select = list(COLUMNS_TO_KEEP_AND_RENAME.values())
    existing_columns = [col for col in columns_to_select if col in df.columns]
    missing_columns = [col for col in columns_to_select if col not in df.columns]
    if missing_columns: logging.warning(f"Columns not found, skipped: {missing_columns}")
    rename_mapping = {v: k for k, v in COLUMNS_TO_KEEP_AND_RENAME.items() if v in existing_columns}
    df_selected = df.select(existing_columns).rename(rename_mapping)
    logging.info(f"Selected columns. Shape: {df_selected.shape}")
    return df_selected

def _clean_cast_validate_impute(df: pl.DataFrame) -> pl.DataFrame:
    """Cleans types, validates ranges, handles specific nulls/empty strings, imputes counts."""
    logging.info("Helper: Cleaning types, validating, imputing...")
    current_date = datetime.date.today()
    df_validated = df.with_columns([ # Casting
        pl.col("latitude").cast(pl.Float64, strict=False),
        pl.col("longitude").cast(pl.Float64, strict=False),
        pl.col("depth_m").cast(pl.Float64, strict=False),
        pl.col("aphia_id").cast(pl.Int64, strict=False),
        pl.col("individual_count").cast(pl.Int32, strict=False),
        pl.col("event_date").str.strptime(pl.Date, "%Y-%m-%d", strict=False),
    ]).with_columns([ # Validation
        pl.when((pl.col("latitude") >= -90) & (pl.col("latitude") <= 90)).then(pl.col("latitude")).otherwise(None).alias("latitude"),
        pl.when((pl.col("longitude") >= -180) & (pl.col("longitude") <= 180)).then(pl.col("longitude")).otherwise(None).alias("longitude"),
        pl.when((pl.col("depth_m") >= 1000) & (pl.col("depth_m") < 12000)).then(pl.col("depth_m")).otherwise(None).alias("depth_m"),
        pl.when((pl.col("event_date").is_not_null()) & (pl.col("event_date").dt.year() >= 1800) & (pl.col("event_date") <= current_date)).then(pl.col("event_date")).otherwise(None).alias("event_date"),
        pl.when(pl.col("scientific_name").str.strip_chars() != "").then(pl.col("scientific_name")).otherwise(None).alias("scientific_name"),
    ]).with_columns([ # Imputation
        pl.col("individual_count").fill_null(1),
    ])
    return df_validated

def _enrich_data(df: pl.DataFrame) -> pl.DataFrame:
    """Adds derived columns and converts timestamps."""
    logging.info("Helper: Enriching data (depth zone, datetimes)...")
     # Check if timestamp columns exist before trying to convert/drop
    cols_to_drop = []
    transform_expressions = []
     
    if "depth_m" in df.columns:
        transform_expressions.append(
             pl.when(pl.col("depth_m").is_null()).then(pl.lit(None, dtype=pl.Utf8))
            .when(pl.col("depth_m") >= 6000).then(pl.lit("Hadal"))
            .when(pl.col("depth_m") >= 4000).then(pl.lit("Abyssal"))
            .when(pl.col("depth_m") >= 1000).then(pl.lit("Bathyal"))
            .otherwise(pl.lit("Less than 1000m/Invalid")).alias("depth_zone")
        )
    else:
        logging.warning("Column 'depth_m' not found, skipping depth_zone enrichment.")
         
    ts_cols = {"event_ts_start": "event_datetime_start", 
                "event_ts_mid": "event_datetime_mid", 
                "event_ts_end": "event_datetime_end"}
                
    for ts_col, dt_col in ts_cols.items():
        if ts_col in df.columns:
            transform_expressions.append(pl.from_epoch(pl.col(ts_col), time_unit="ms").alias(dt_col))
            cols_to_drop.append(ts_col)
        else:
            logging.warning(f"Timestamp column '{ts_col}' not found, skipping conversion.")
               
    if not transform_expressions:
        logging.warning("No enrichment or timestamp conversions to apply.")
        return df # Return unchanged if nothing to do
         
    df_enriched = df.with_columns(transform_expressions)
     
    # Drop original timestamp columns if they existed and were converted
    if cols_to_drop:
        df_enriched = df_enriched.drop(cols_to_drop)
         
    return df_enriched

try:
    from .obis_sql_schema import ALL_TABLE_CREATE_STATEMENTS 
    from .obis_normalization import extract_species_dimension, extract_datasets_dimension, extract_record_details_dimension, create_occurrence_facts
    from .obis_db_operations import create_table, load_table_from_parquet
    
    IMPORTS_OK = True
except ImportError as e:
    logging.error(f"Failed to import dependent modules (schema/normalization/db_ops): {e}")
    IMPORTS_OK = False
    
def transform_and_split_data(raw_data_path: str) -> dict[str, pl.DataFrame] | None:
    """ Orchestrates OBIS data transformation AND normalization into dimension/fact tables. """
    if not IMPORTS_OK: raise ImportError("ETL/Normalization/Schema functions failed to load.")
    logging.info(f"--- Starting Full Transformation & Normalization for {raw_data_path} ---")
    try:
        # Initial Load & Clean using existing helpers
        df_dedup = _load_and_deduplicate_raw_data(raw_data_path)
        if df_dedup is None or df_dedup.height == 0: return None 
        df_selected = _select_and_rename(df_dedup)
        df_validated = _clean_cast_validate_impute(df_selected)
        df_enriched = _enrich_data(df_validated)
        if df_enriched is None or df_enriched.height == 0: return None
        logging.info(f"Base cleaning finished. Shape before normalization: {df_enriched.shape}")

        # Extract Dimensions & Facts using imported normalization functions
        df_species = extract_species_dimension(df_enriched)
        df_datasets = extract_datasets_dimension(df_enriched)
        df_records = extract_record_details_dimension(df_enriched)
        df_facts= create_occurrence_facts(df_enriched)

        logging.info(f"--- Transformation & Normalization Finished ---")
        return {
            'species': df_species, 
            'datasets': df_datasets,
            'record_details': df_records,
            'occurrences': df_facts 
        }
    except Exception as e:
        logging.error(f"Error during transform/split: {e}", exc_info=True)
        return None
    
def prepare_postgres_schema(conn_str: str):
    """ Ensures all tables defined in obis_schema.py exist. """
    if not IMPORTS_OK: raise ImportError("ETL/DB Ops/Schema functions failed to load.")
    logging.info(f"Ensuring PostgreSQL schema exists using definitions from obis_schema.py...")
    try:
        engine = create_engine(conn_str) 
        for i, create_sql in enumerate(ALL_TABLE_CREATE_STATEMENTS):
            table_name_guess = f"table_{i+1}" 
            create_table(engine, create_sql, table_name_guess)
        logging.info(f"All {len(ALL_TABLE_CREATE_STATEMENTS)} table schemas ensured to exist.")
        return True
    except Exception as e:
        raise RuntimeError("Failed to ensure PostgreSQL schema exists.") from e
    
def load_postgres_normalized_data(conn_id: str, data_path_prefix: str):
        
    logging.info("--- Starting Normalized Data Load (Pre-filter Append Dims, Replace Facts) ---")
    
    # Define dimensions and their primary keys
    dimensions_map = {
        "species": {"target_table": "dim_species", "pk": "aphia_id"},
        "datasets": {"target_table": "dim_datasets", "pk": "dataset_id"},
        "records": {"target_table": "dim_record_details", "pk": "obis_id"},
    }
    fact_file_prefix = "occurrences"
    fact_table_name = "fact_occurrences"
    fact_load_strategy = "replace"

    # --- Load Dimensions (Check Existing Keys, Filter, then Append) ---
    logging.info("Loading/Updating Dimension Tables (Insert New PKs Only)...")
    all_dims_succeeded = True
    pg_hook = PostgresHook(postgres_conn_id=conn_id)
    conn_str = pg_hook.get_uri()

    for file_prefix, config in dimensions_map.items():
        parquet_file = os.path.join(data_path_prefix, f"{file_prefix}.parquet")
        target_table = config["target_table"]
        pk_col = config["pk"]
        
        logging.info(f"--- Processing Dimension: {target_table} ---")
        
        if not os.path.exists(parquet_file):
            logging.warning(f"Parquet file not found: {parquet_file}. Skipping dimension {target_table}.")
            continue 

        try:
            df_new_dim = pl.read_parquet(parquet_file)
            
            if df_new_dim.height > 0:
                if pk_col not in df_new_dim.columns:
                     logging.error(f"PK column '{pk_col}' not found in {parquet_file}. Skipping {target_table}.")
                     all_dims_succeeded = False; continue
                
                df_new_dim = df_new_dim.drop_nulls(subset=[pk_col])
                if df_new_dim.height == 0:
                     logging.info(f"No valid non-null PKs found in {parquet_file} for {target_table}. Skipping.")
                     continue

                # Get existing primary keys from target table
                logging.info(f"Fetching existing keys from public.{target_table}...")
                sql_get_keys = f'SELECT DISTINCT "{pk_col}" FROM public."{target_table}";'
                existing_keys_df = pg_hook.get_pandas_df(sql=sql_get_keys) # Fetch keys
                existing_keys_set = set(existing_keys_df[pk_col])
                logging.info(f"Found {len(existing_keys_set)} existing keys.")

                # Filter new data to exclude existing keys
                df_to_insert = df_new_dim.filter(~pl.col(pk_col).is_in(existing_keys_set))
                num_to_insert = df_to_insert.height
                
                if num_to_insert > 0:
                    # Append only the *new* rows using write_database
                    logging.info(f"Found {num_to_insert} new records for {target_table}. Appending...")
                    df_to_insert.write_database(
                        table_name=target_table,
                        connection=conn_str,
                        if_table_exists="append", # Append to the existing table
                    )
                    logging.info(f"Append complete for {target_table}.")
                else:
                    logging.info(f"No new records to insert into {target_table}.")
            else:
                 logging.info(f"Parquet file {parquet_file} for dimension {target_table} is empty. Skipping.")

        except Exception as e:
            logging.error(f"Failed processing dimension {target_table}: {e}")
            all_dims_succeeded = False
            raise

    # --- Load Fact Table ---
    if all_dims_succeeded:
         logging.info(f"Loading Fact Table ({fact_table_name}, strategy: {fact_load_strategy})...")
         fact_parquet_file = os.path.join(data_path_prefix, f"{fact_file_prefix}.parquet")
         success = load_table_from_parquet(fact_parquet_file, fact_table_name, conn_str, if_table_exists_strategy=fact_load_strategy) 
         if not success:
              pass
    else:
         logging.error("Skipping fact table load due to dimension processing issues.")
         raise RuntimeError("Dimension table processing failed or was skipped.")

    logging.info("--- Normalized Data Load Finished ---")
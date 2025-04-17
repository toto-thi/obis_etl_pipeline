import os
import requests
import logging
import time
import datetime
import polars as pl

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

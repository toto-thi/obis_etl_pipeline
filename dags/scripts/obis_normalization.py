import polars as pl
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def extract_species_dimension(df: pl.DataFrame) -> pl.DataFrame:
    """Extracts unique species data based on aphia_id."""
    logging.info("Extracting Species dimension...")
    if "aphia_id" not in df.columns:
        logging.error("Cannot extract species dimension: 'aphia_id' column missing.")
        return pl.DataFrame() 

    # Select & ensure relevant columns exist before selecting
    species_cols = ["aphia_id", "scientific_name", "kingdom", "phylum", "class_name", "order_name", "family"]
    existing_species_cols = [col for col in species_cols if col in df.columns]
    
    df_species = df.select(existing_species_cols).filter(
        pl.col("aphia_id").is_not_null() # set aphia_id as the primary key
    ).unique(
        subset=["aphia_id"], 
        keep="first", # Keep first encountered details for a given species ID
        maintain_order=False 
    )
    logging.info(f"Extracted {df_species.height} unique species records.")
    return df_species

def extract_datasets_dimension(df: pl.DataFrame) -> pl.DataFrame:
    """Extracts unique dataset data based on dataset_id."""
    logging.info("Extracting Datasets dimension...")
    if "dataset_id" not in df.columns:
        logging.error("Cannot extract datasets dimension: 'dataset_id' column missing.")
        return pl.DataFrame()

    # Select relevant columns
    dataset_cols = ["dataset_id", "references_url"]
    existing_dataset_cols = [col for col in dataset_cols if col in df.columns]

    df_datasets = df.select(existing_dataset_cols).filter(
        pl.col("dataset_id").is_not_null()
    ).unique(
        subset=["dataset_id"], 
        keep="first",
        maintain_order=False
    )
    logging.info(f"Extracted {df_datasets.height} unique dataset records.")
    return df_datasets

def extract_record_details_dimension(df: pl.DataFrame) -> pl.DataFrame:
    """Extracts unique record detail data based on obis_id."""
    logging.info("Extracting Record Details dimension...")
    if "obis_id" not in df.columns:
        logging.error("Cannot extract record details dimension: 'obis_id' column missing.")
        return pl.DataFrame()

    # Select relevant columns
    record_cols = ["obis_id", "basis_of_record", "occurrence_status"]
    existing_record_cols = [col for col in record_cols if col in df.columns]
    
    df_records = df.select(existing_record_cols).filter(
        pl.col("obis_id").is_not_null()
    ).unique(
        subset=["obis_id"], 
        keep="first",
        maintain_order=False
    )
    logging.info(f"Extracted {df_records.height} unique record detail records.")
    return df_records

def create_occurrence_facts(df: pl.DataFrame) -> pl.DataFrame:
    """Selects columns for the occurrences fact table."""
    logging.info("Creating Occurrences fact table structure...")
    
    fact_cols = [
        "occurrence_id", # PK
        "obis_id",       # FK to dim_record_details
        "aphia_id",      # FK to dim_species
        "dataset_id",    # FK to dim_datasets
        "latitude", 
        "longitude", 
        "depth_m", 
        "event_date", 
        "event_datetime_mid",
        "individual_count", 
        "depth_zone",
    ]
    
    existing_fact_cols = [col for col in fact_cols if col in df.columns]
    missing_fact_cols = [col for col in fact_cols if col not in df.columns]
    if missing_fact_cols:
         logging.warning(f"Columns missing for fact table: {missing_fact_cols}")

    df_facts = df.select(existing_fact_cols)
    logging.info(f"Created facts structure with {df_facts.height} rows.")
    return df_facts
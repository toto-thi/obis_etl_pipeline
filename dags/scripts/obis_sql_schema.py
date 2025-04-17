SQL_CREATE_DIM_SPECIES = """
CREATE TABLE IF NOT EXISTS dim_species (
    aphia_id BIGINT PRIMARY KEY, 
    scientific_name TEXT,
    kingdom VARCHAR(100),
    phylum VARCHAR(100),
    class_name VARCHAR(100),
    order_name VARCHAR(100),
    family VARCHAR(100)
);
"""

SQL_CREATE_DIM_DATASETS = """
CREATE TABLE IF NOT EXISTS dim_datasets (
    dataset_id TEXT PRIMARY KEY,
    references_url TEXT
);
"""

SQL_CREATE_DIM_RECORD_DETAILS = """
CREATE TABLE IF NOT EXISTS dim_record_details (
    obis_id TEXT PRIMARY KEY,
    basis_of_record VARCHAR(100),
    occurrence_status VARCHAR(100)
);
"""

SQL_CREATE_FACT_OCCURRENCES = """
CREATE TABLE IF NOT EXISTS fact_occurrences (
    occurrence_id VARCHAR(255) PRIMARY KEY,
    obis_id TEXT, 
    aphia_id BIGINT, 
    dataset_id TEXT, 
    latitude FLOAT8,
    longitude FLOAT8,
    depth_m FLOAT8,
    event_date DATE,
    event_datetime_mid TIMESTAMP, 
    individual_count INTEGER,
    depth_zone VARCHAR(50),
    -- Foreign Key Constraints 
    CONSTRAINT fk_obis FOREIGN KEY(obis_id) REFERENCES dim_record_details(obis_id) ON DELETE SET NULL,
    CONSTRAINT fk_aphia FOREIGN KEY(aphia_id) REFERENCES dim_species(aphia_id) ON DELETE SET NULL,
    CONSTRAINT fk_dataset FOREIGN KEY(dataset_id) REFERENCES dim_datasets(dataset_id) ON DELETE SET NULL
);
"""

ALL_TABLE_CREATE_STATEMENTS = [
    SQL_CREATE_DIM_SPECIES,
    SQL_CREATE_DIM_DATASETS,
    SQL_CREATE_DIM_RECORD_DETAILS,
    SQL_CREATE_FACT_OCCURRENCES,
]
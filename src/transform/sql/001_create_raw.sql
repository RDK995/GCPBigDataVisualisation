CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_RAW_DATASET}`;
CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_STAGING_DATASET}`;
CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_MART_DATASET}`;

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.${WORLD_BANK_RAW_TABLE}` (
  ingested_at TIMESTAMP,
  extract_run_id STRING,
  source_file STRING,
  indicator_id STRING,
  payload JSON
)
PARTITION BY DATE(ingested_at)
CLUSTER BY indicator_id, extract_run_id;

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.${OPEN_METEO_RAW_TABLE}` (
  ingested_at TIMESTAMP,
  extract_run_id STRING,
  source_file STRING,
  location_id STRING,
  latitude FLOAT64,
  longitude FLOAT64,
  payload JSON
)
PARTITION BY DATE(ingested_at)
CLUSTER BY location_id, extract_run_id;

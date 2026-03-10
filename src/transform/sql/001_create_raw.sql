-- Raw landing table preserving full source payload.
CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT_ID}.raw`;

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.raw.api_entities_raw` (
  payload JSON,
  source_file STRING,
  extract_run_id STRING,
  ingested_at TIMESTAMP,
  source_name STRING
)
PARTITION BY DATE(ingested_at)
CLUSTER BY extract_run_id, source_name;

-- TODO(model): adjust schema when you know stable source metadata fields.

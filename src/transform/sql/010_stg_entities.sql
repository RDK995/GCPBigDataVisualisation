-- Stage typed fields from raw payload.
CREATE SCHEMA IF NOT EXISTS `${GCP_PROJECT_ID}.staging`;

CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.staging.stg_entities` (
  entity_id STRING,
  entity_name STRING,
  status STRING,
  updated_at TIMESTAMP,
  source_ingested_at TIMESTAMP,
  extract_run_id STRING
)
PARTITION BY DATE(updated_at)
CLUSTER BY entity_id, status;

MERGE `${GCP_PROJECT_ID}.staging.stg_entities` AS target
USING (
  SELECT
    JSON_VALUE(payload, '$.id') AS entity_id,
    JSON_VALUE(payload, '$.name') AS entity_name,
    JSON_VALUE(payload, '$.status') AS status,
    TIMESTAMP(JSON_VALUE(payload, '$.updated_at')) AS updated_at,
    ingested_at AS source_ingested_at,
    extract_run_id,
    ROW_NUMBER() OVER (
      PARTITION BY JSON_VALUE(payload, '$.id')
      ORDER BY TIMESTAMP(JSON_VALUE(payload, '$.updated_at')) DESC, ingested_at DESC
    ) AS row_num
  FROM `${GCP_PROJECT_ID}.raw.api_entities_raw`
  WHERE JSON_VALUE(payload, '$.id') IS NOT NULL
) AS source
ON target.entity_id = source.entity_id
WHEN MATCHED AND source.row_num = 1 THEN
  UPDATE SET
    entity_name = source.entity_name,
    status = source.status,
    updated_at = source.updated_at,
    source_ingested_at = source.source_ingested_at,
    extract_run_id = source.extract_run_id
WHEN NOT MATCHED AND source.row_num = 1 THEN
  INSERT (entity_id, entity_name, status, updated_at, source_ingested_at, extract_run_id)
  VALUES (source.entity_id, source.entity_name, source.status, source.updated_at, source.source_ingested_at, source.extract_run_id);

-- TODO(model): map source fields and data types for your API schema.

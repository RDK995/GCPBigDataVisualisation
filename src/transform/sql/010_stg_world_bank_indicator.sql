CREATE TABLE IF NOT EXISTS `${GCP_PROJECT_ID}.${BQ_STAGING_DATASET}.stg_world_bank_indicator` (
  country_iso3 STRING,
  country_name STRING,
  indicator_id STRING,
  indicator_name STRING,
  year INT64,
  value_num FLOAT64,
  ingested_at TIMESTAMP,
  extract_run_id STRING
)
PARTITION BY RANGE_BUCKET(year, GENERATE_ARRAY(1900, 2200, 1))
CLUSTER BY country_iso3, indicator_id;

MERGE `${GCP_PROJECT_ID}.${BQ_STAGING_DATASET}.stg_world_bank_indicator` AS target
USING (
  SELECT
    JSON_VALUE(payload, '$.countryiso3code') AS country_iso3,
    JSON_VALUE(payload, '$.country.value') AS country_name,
    JSON_VALUE(payload, '$.indicator.id') AS indicator_id,
    JSON_VALUE(payload, '$.indicator.value') AS indicator_name,
    SAFE_CAST(JSON_VALUE(payload, '$.date') AS INT64) AS year,
    SAFE_CAST(JSON_VALUE(payload, '$.value') AS FLOAT64) AS value_num,
    ingested_at,
    extract_run_id,
    ROW_NUMBER() OVER (
      PARTITION BY
        JSON_VALUE(payload, '$.countryiso3code'),
        JSON_VALUE(payload, '$.indicator.id'),
        SAFE_CAST(JSON_VALUE(payload, '$.date') AS INT64)
      ORDER BY ingested_at DESC, extract_run_id DESC
    ) AS row_num
  FROM `${GCP_PROJECT_ID}.${BQ_RAW_DATASET}.${WORLD_BANK_RAW_TABLE}`
  WHERE JSON_VALUE(payload, '$.countryiso3code') IS NOT NULL
    AND JSON_VALUE(payload, '$.indicator.id') IS NOT NULL
    AND JSON_VALUE(payload, '$.date') IS NOT NULL
    AND SAFE_CAST(JSON_VALUE(payload, '$.date') AS INT64) IS NOT NULL
) AS source
ON target.country_iso3 = source.country_iso3
  AND target.indicator_id = source.indicator_id
  AND target.year = source.year
WHEN MATCHED AND source.row_num = 1 THEN
  UPDATE SET
    country_name = source.country_name,
    indicator_name = source.indicator_name,
    value_num = source.value_num,
    ingested_at = source.ingested_at,
    extract_run_id = source.extract_run_id
WHEN NOT MATCHED AND source.row_num = 1 THEN
  INSERT (
    country_iso3,
    country_name,
    indicator_id,
    indicator_name,
    year,
    value_num,
    ingested_at,
    extract_run_id
  )
  VALUES (
    source.country_iso3,
    source.country_name,
    source.indicator_id,
    source.indicator_name,
    source.year,
    source.value_num,
    source.ingested_at,
    source.extract_run_id
  );
